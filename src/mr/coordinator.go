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

//worker state
const (
	Idle 		=0
	Working 	=1
	Dead 		=2
)

//task type
const (
	TypeMap 	=0
	TypeReduce	=1
)

//task state
const (
	Unassigned	=0
	Assigned 	=1
	Finished	=2
)

type Coordinator struct {
	sync.RWMutex
	workers 		map[int]*WorkerInfo
	tasks 			map[int]*TaskInfo
	
	workerCount 	int
	reducerCount	int

	mapCount 		int

	done 			bool
}

type WorkerInfo	struct {
	// sync.RWMutex	
	state 			int
	startTime		time.Time
	lastTask		int
}

type TaskInfo struct {
	// sync.RWMutex
	workerId 		int
	taskType 		int
	state 			int
	fileName 		string
}

func (c *Coordinator) RPCAssignTask(args AssignTaskArgs, reply *AssignTaskReply) error {
	
	c.Lock()
	defer c.Unlock()

	var success bool = false
	var tid 	int

	for id, task := range c.tasks {
		// task.Lock()
		if task.state == Unassigned {
			task.state = Assigned
			task.workerId = args.Workerid

			reply.TaskId = id
			reply.TaskType = task.taskType
			reply.FileName = task.fileName
			reply.ReducerCount = c.reducerCount
			reply.MapCount = c.mapCount

			s := ""
			if task.taskType == TypeMap {
				s += "map"
			}else {
				s += "reduce"
			}

			log.Printf("worker %v will start working %v task %v\n", args.Workerid, s, id)
			
			success = true
			tid = id
		}
		// task.Unlock()

		if success {
			worker, _ := c.workers[args.Workerid]
			// worker.Lock()
			worker.lastTask = tid 
			worker.state = Working
			worker.startTime = time.Now()
			// worker.Unlock()
			return nil 
		}
	}

	//There is no task to do now
	reply.TaskId = -1
	return nil

}

func (c *Coordinator) RPCFinishTask(args FinishTaskArgs, reply *FinishTaskReply) error {
	
	c.Lock()
	defer c.Unlock()

	worker, _ := c.workers[args.Workerid]
	worker.state = Idle

	tid := worker.lastTask
	task, _ := c.tasks[tid]
	task.state = Finished
	
	s := ""
	if task.taskType == TypeMap {
		s += "map"
	}else{
		s += "reduce"
	}

	log.Printf("worker %v finished %v task %v\n", args.Workerid, s, tid)

	//Check if all the tasks have finished
	finished := true
	var taskType int
	for _, task := range c.tasks {
		if task.state != Finished {
			finished = false
			break
		}

		taskType = task.taskType
	}

	if finished {
		if taskType == TypeMap {
			c.tasks = make(map[int]*TaskInfo)

			for i:=0; i<c.reducerCount; i++ {
				c.tasks[i] = &TaskInfo{
					taskType: TypeReduce,
					state: Unassigned,
				}
			}
		}else{
			//Finish all the task 
			c.done = true
		}
	}
	
	return nil
}


func (c *Coordinator) RPCRegisterWorker(args RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.Lock()
	defer c.Unlock()
	id := c.workerCount
	c.workerCount++
	c.workers[id] = &WorkerInfo{
		state : Idle,
		lastTask: -1,
	}
	reply.Workerid = id
	log.Printf("register new worker %v\n", id)
	return nil
}

// func (c *Coordinator) WorkerCheck() {}


//Done main/mrcoordinator.go calls Done() periodically to find out
//if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.done
}


//MakeCoordinator creates a Coordinator.
//main/mrcoordinator.go calls this function.
//nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		workers		: make(map[int]*WorkerInfo),
		tasks 		: make(map[int]*TaskInfo),		
		workerCount	: 0,
		reducerCount: nReduce,
		mapCount	: len(files),
		done 		: false,
	}

	//register map tasks
	//each file registers a task
	for i, file := range files {
		c.tasks[i] = &TaskInfo{
			workerId	: -1,
			taskType	: TypeMap,
			state		: Unassigned,
			fileName	: file,
		}
	}


	c.server()
	return &c
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