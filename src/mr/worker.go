package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		JobsDone := DoJob(JobGetArgs{true}, mapf, nil)
		if JobsDone {
			break
		}
		time.Sleep(time.Second)
	}
	for {
		JobsDone := DoJob(JobGetArgs{false}, nil, reducef)
		if JobsDone {
			break
		}
		time.Sleep(time.Second)
	}

}

func DoJob(args JobGetArgs, mapf func(string, string) []KeyValue, reducef func(string, []string) string) bool {
	taskId, path, nReduce, jobsDone, reduceFileNames, bucketId := CallGetJob(args)
	if jobsDone {
		return true
	}
	if args.IsMapJob {
		if path == "" {
			return false
		}
		data, err := os.ReadFile(path)
		if err != nil {
			panic(err)
		}
		fileContents := string(data)
		dataMap := mapf(path, fileContents)
		buckets := make([][]KeyValue, nReduce)
		for _, keyValue := range dataMap {
			i := ihash(keyValue.Key) % nReduce
			buckets[i] = append(buckets[i], keyValue)
		}
		CreateMappedFiles(buckets, path)
		CallJobFinished(taskId, true)
	} else {
		if reduceFileNames == nil {
			return false
		}
		bucket := GetBucket(bucketId, reduceFileNames)
		mappedValues := make(map[string][]string)
		resKeyValues := []KeyValue{}
		for _, keyValue := range bucket {
			mappedValues[keyValue.Key] = append(mappedValues[keyValue.Key], keyValue.Value)
		}
		for key, values := range mappedValues {
			resKeyValues = append(resKeyValues, KeyValue{key, reducef(key, values)})

		}
		WriteOutput(resKeyValues, bucketId)
		CallJobFinished(taskId, false)

	}

	return false
}

func CallJobFinished(taskId int, isMapTask bool) {
	args := JobDoneArgs{taskId, isMapTask}
	reply := JobGetReply{}
	ok := call("Coordinator.JobFinished", &args, &reply)
	if !ok {
		panic("Could not notify coordinator: JobFinished!")
	}
}

func WriteOutput(resKeyValues []KeyValue, bucketId int) {
	fileName := "mr-out-" + strconv.Itoa(bucketId)
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Could not create/open file: "+fileName, err)
	}
	for _, keyValue := range resKeyValues {
		fmt.Fprintf(file, "%v %v\n", keyValue.Key, keyValue.Value)
	}
	file.Close()
}

func GetBucket(bucketId int, files []string) []KeyValue {

	mainBucket := []KeyValue{}
	for _, file := range files {

		data, err := os.ReadFile(file)
		if err != nil {
			panic(err)
		}
		bucket := []KeyValue{}
		if json.Unmarshal(data, &bucket) != nil {
			fmt.Fprintln(os.Stderr, "Problem in decoding json!")
		}
		mainBucket = append(mainBucket, bucket...)
	}
	return mainBucket
}

func CreateMappedFiles(buckets [][]KeyValue, path string) {
	for i, bucket := range buckets {
		data, err := json.Marshal(bucket)
		if err != nil {
			panic(err)
		}
		err = os.WriteFile(GetMidFileName(path, i), data, 0644)
		if err != nil {
			panic(err)
		}

	}
}

func GetMidFileName(path string, i int) string {
	dir, fileName := filepath.Split(path)
	return dir + strconv.Itoa(i) + "-" + fileName
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallGetJob(args JobGetArgs) (int, string, int, bool, []string, int) {

	reply := JobGetReply{}
	ok := call("Coordinator.GetJob", &args, &reply)
	if ok {
		return reply.TaskId, reply.FileName, reply.NReduce, reply.JobsDone, reply.ReduceFileNames, reply.BucketId
	} else {
		return 0, "", 0, false, []string{}, 0
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
