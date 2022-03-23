package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Task_Idle = iota
	Task_In_Progress
	Task_Comleted
	Task_Type_Map
	Task_Type_Reduce
)

type Task struct {
	Id        int
	Status    int
	Worker    int
	Task_type int
}

type MapTask struct {
	Task
	FileName    string
	MidFileName string
}

type ReduceTask struct {
	Task
	FinalFileName string
}

type WorkerInfo struct {
	Finish_tasks []int
	Progressing  int
	Alive        bool
}

type Coordinator struct {
	// Your definitions here.
	MapTasks          []MapTask
	ReduceTasks       []ReduceTask
	Workers           []WorkerInfo
	MapTaskQueue      []int
	MapTaskCur        int
	ReduceTaskQueue   []int
	ReduceTaskCur     int
	CntWorker         int
	R                 int
	M                 int
	WaitTask          *sync.Cond
	Finish            bool
	FinishMapCount    int
	FinishReduceCount int
	RegisterMutex     sync.Mutex
	FinishMutex       sync.RWMutex
	StateCheckMutex   sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.RegisterMutex.Lock()
	defer c.RegisterMutex.Unlock()
	c.CntWorker++
	reply.WorkerId = c.CntWorker
	reply.M = c.M
	reply.R = c.R
	c.Workers = append(c.Workers, WorkerInfo{Progressing: -1, Alive: true})
	return nil
}

//c.WaitTask.L need to be lock
func (c *Coordinator) Redo(task_type int, task_id int) {
	is_full := false
	if task_type == Task_Type_Map {
		if !c.Finish && c.MapTaskCur == len(c.MapTaskQueue) && (c.FinishMapCount != len(c.MapTasks) || c.ReduceTaskCur == len(c.ReduceTaskQueue)) {
			is_full = true
		}
		c.MapTasks[task_id].Status = Task_Idle
		c.MapTaskQueue = append(c.MapTaskQueue, task_id)
	} else if task_type == Task_Type_Reduce {
		if !c.Finish && c.MapTaskCur == len(c.MapTaskQueue) && (c.FinishMapCount == len(c.MapTasks) && c.ReduceTaskCur == len(c.ReduceTaskQueue)) {
			is_full = true
		}
		c.ReduceTasks[task_id].Status = Task_Idle
		c.ReduceTaskQueue = append(c.ReduceTaskQueue, task_id)
	} else {
		fmt.Print(errors.New("Wrong task type!"))
	}
	if is_full {
		c.WaitTask.Signal()
	}
}

func (c *Coordinator) CheckAlive(task_type int, task_id int) {
	time.Sleep(10 * time.Second)
	c.WaitTask.L.Lock()
	defer c.WaitTask.L.Unlock()
	if task_type == Task_Type_Map {
		if c.MapTasks[task_id].Status != Task_Comleted {
			//w := c.MapTasks[task_id].Worker
			c.Redo(task_type, task_id)
			/*
				for _, id := range c.Workers[w].Finish_tasks {
					c.Redo(Task_Type_Map, id)
				}
			*/
		}
	} else if task_type == Task_Type_Reduce {
		if c.ReduceTasks[task_id].Status != Task_Comleted {
			//w := c.ReduceTasks[task_id].Worker
			/*
				for _, id := range c.Workers[w].Finish_tasks {
					c.Redo(Task_Type_Map, id)
				}
			*/
			c.Redo(Task_Type_Reduce, task_id)
		}
	} else {
		fmt.Println(errors.New("Wrong task type!"))
	}
}

func (c *Coordinator) AskForTask(args *AskArgs, reply *AskReply) error {
	c.WaitTask.L.Lock()
	defer c.WaitTask.L.Unlock()
	if !c.Finish && c.MapTaskCur == len(c.MapTaskQueue) && (c.FinishMapCount != len(c.MapTasks) || c.ReduceTaskCur == len(c.ReduceTaskQueue)) {
		c.WaitTask.Wait()
	}
	if c.Finish {
		reply.Over = true
	} else if c.MapTaskCur < len(c.MapTaskQueue) {
		id := c.MapTaskQueue[c.MapTaskCur]
		c.MapTaskCur++
		reply.Over = false
		reply.TaskType = 0
		reply.TaskId = id
		reply.MapFileName = c.MapTasks[id].FileName
		c.MapTasks[id].Status = Task_In_Progress
		c.MapTasks[id].Worker = args.WorkerId
		go c.CheckAlive(Task_Type_Map, id)
	} else if c.FinishMapCount == len(c.MapTasks) && c.ReduceTaskCur < len(c.ReduceTaskQueue) {
		reply.Over = false
		reply.TaskType = 1
		reply.TaskId = c.ReduceTaskQueue[c.ReduceTaskCur]
		c.ReduceTaskCur++
		go c.CheckAlive(Task_Type_Reduce, reply.TaskId)
	} else {
		err := errors.New("Unconditioned wake up!")
		log.Fatalln(err)
		return err
	}
	return nil
}

func (c *Coordinator) FinishMap(args *FinishMapArgs, reply *FinishMapReply) error {
	c.WaitTask.L.Lock()
	defer c.WaitTask.L.Unlock()
	if c.MapTasks[args.TaskId].Status != Task_Comleted {
		c.MapTasks[args.TaskId].Status = Task_Comleted
		//c.MapTasks[args.TaskId].MidFileName = args.FileName
		c.MapTasks[args.TaskId].MidFileName = fmt.Sprintf("mr-inter-%d", args.TaskId)
		c.FinishMapCount++
		if c.FinishMapCount == len(c.MapTasks) && c.ReduceTaskCur < len(c.ReduceTaskQueue) {
			c.WaitTask.Signal()
		}
	}
	return nil
}

func (c *Coordinator) FinishReduce(args *FinishReduceArgs, reply *FinishReduceReply) error {
	c.WaitTask.L.Lock()
	defer c.WaitTask.L.Unlock()
	if c.ReduceTasks[args.TaskId].Status != Task_Comleted {
		c.ReduceTasks[args.TaskId].Status = Task_Comleted
		//c.MapTasks[args.TaskId].MidFileName = args.FileName
		c.ReduceTasks[args.TaskId].FinalFileName = fmt.Sprintf("mr-out-%d", args.TaskId)
		c.FinishReduceCount++
		reply.Over = false
		if c.FinishReduceCount == c.R {
			c.FinishMutex.Lock()
			c.Finish = true
			c.FinishMutex.Unlock()
			reply.Over = true
			c.WaitTask.Broadcast()
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.FinishMutex.RLock()
	defer c.FinishMutex.RUnlock()
	ret := c.Finish
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.M = len(files)
	c.R = nReduce
	c.MapTasks = make([]MapTask, len(files))
	c.FinishMapCount = 0
	c.MapTaskQueue = make([]int, len(files))
	for i, f := range files {
		c.MapTasks[i].Id = i
		c.MapTasks[i].FileName = f
		c.MapTasks[i].Status = Task_Idle
		c.MapTasks[i].Task_type = Task_Type_Map
		c.MapTasks[i].Worker = -1
		c.MapTaskQueue[i] = i
	}

	c.ReduceTasks = make([]ReduceTask, nReduce)
	c.FinishReduceCount = 0
	c.ReduceTaskQueue = make([]int, nReduce)

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i].Id = i
		c.ReduceTasks[i].Status = Task_Idle
		c.ReduceTasks[i].Task_type = Task_Type_Reduce
		c.ReduceTasks[i].Worker = -1
		c.ReduceTaskQueue[i] = i
	}
	c.WaitTask = sync.NewCond(&sync.Mutex{})
	c.server()
	return &c
}
