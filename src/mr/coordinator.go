package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	AVAILABLE int = iota
	PROCESSING
	FINISHED
)

type ReduceTask struct {
	fileNames []string
	status    int
}
type MapTask struct {
	fileName string
	status   int
}
type Coordinator struct {
	mapTasks        []MapTask
	reduceTasks     []ReduceTask
	nReduce         int
	nMapJobsLeft    int
	nReduceJobsLeft int
	mutex           sync.Mutex
}

func (c *Coordinator) TimeoutHandler(taskId int, isMapTask bool) {
	time.Sleep(time.Second * 10)
	c.mutex.Lock()
	if isMapTask && c.mapTasks[taskId].status != FINISHED {
		c.mapTasks[taskId].status = AVAILABLE

	} else if !isMapTask && c.reduceTasks[taskId].status != FINISHED {
		c.reduceTasks[taskId].status = AVAILABLE
	}
	c.mutex.Unlock()
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetJob(args *JobGetArgs, reply *JobGetReply) error {
	if args.IsMapJob {
		JobsDone := true
		c.mutex.Lock()
		for taskId := 0; taskId < len(c.mapTasks); taskId++ {
			task := &c.mapTasks[taskId]
			if task.status != FINISHED {
				JobsDone = false
				if task.status == AVAILABLE {
					*reply = JobGetReply{NReduce: c.nReduce, TaskId: taskId, FileName: task.fileName}

					task.status = PROCESSING
					go c.TimeoutHandler(taskId, true)
					break
				}
			}
		}
		c.mutex.Unlock()
		reply.JobsDone = JobsDone
	} else {
		JobsDone := true
		c.mutex.Lock()
		for taskId := 0; taskId < c.nReduce; taskId++ {
			task := &c.reduceTasks[taskId]
			if task.status != FINISHED {
				JobsDone = false
				if task.status == AVAILABLE {
					*reply = JobGetReply{NReduce: c.nReduce, TaskId: taskId, ReduceFileNames: task.fileNames, BucketId: taskId}
					task.status = PROCESSING
					go c.TimeoutHandler(taskId, false)
					break
				}
			}
		}
		c.mutex.Unlock()
		reply.JobsDone = JobsDone
	}
	return nil
}

func (c *Coordinator) JobFinished(args *JobDoneArgs, reply *JobDoneReply) error {
	if args.IsMapTask {
		c.mutex.Lock()
		c.mapTasks[args.TaskId].status = FINISHED
		c.nMapJobsLeft--
		c.mutex.Unlock()
	} else {
		c.mutex.Lock()
		c.reduceTasks[args.TaskId].status = FINISHED
		c.nReduceJobsLeft--
		c.mutex.Unlock()
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.nMapJobsLeft == 0 && c.nReduceJobsLeft == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce, nMapJobsLeft: len(files), nReduceJobsLeft: nReduce, mapTasks: []MapTask{}, reduceTasks: make([]ReduceTask, nReduce)}
	c.mutex.Lock()
	for _, file := range files {
		c.mapTasks = append(c.mapTasks, MapTask{fileName: file, status: AVAILABLE})
		for i := 0; i < nReduce; i++ {
			c.reduceTasks[i].fileNames = append(c.reduceTasks[i].fileNames, GetMidFileName(file, i))
		}
	}
	c.mutex.Unlock()
	c.server()
	return &c
}
