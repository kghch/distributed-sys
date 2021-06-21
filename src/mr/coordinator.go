package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	//mu sync.Mutex
	idCounter int
	workerIdCount int
	fileNames []string
	mapTasks []Task
	reduceTasks []Task
}

type Task struct {
	Id int
	Ttype int // 0:map, 1:reduce, 2:wait
	FileName string
	Content string // param for mapf

	TmpFiles []string // output of mapf & input of reducef
	OutputFile string // output of reducef

	Status int // 0:not assigned, 1:success, 2:fail, 3:running
	NReduce int
	TaskIds []int
	//WorkerId int
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

func (c *Coordinator) AssignTask(req *TaskReq, reply *TaskResp) error {
	mu.Lock()
	defer mu.Unlock()
	mapFinished := true
	for i, _ := range c.mapTasks {
		if c.mapTasks[i].Status == 0 {
			c.mapTasks[i].Status = 3
			//c.mapTasks[i].WorkerId = req.WorkerId
			reply.Task = &c.mapTasks[i]
			return nil
		}
		if c.mapTasks[i].Status != 1 {
			mapFinished = false
		}
	}

	if !mapFinished {
		reply.Task = &Task{}
		reply.Task.Ttype = 2
		return nil
	}

	for i, _ := range c.reduceTasks {
		if c.reduceTasks[i].Status == 0 {
			c.reduceTasks[i].Status = 3
			//c.reduceTasks[i].WorkerId = req.WorkerId
			reply.Task = &c.reduceTasks[i]
			return nil
		}
	}

	return nil
}

func (c *Coordinator) ReportTask(req *ReportReq, reply *ReportResp) error {
	mu.Lock()
	defer mu.Unlock()
	for i, task := range c.mapTasks {
		if task.Id == req.Task.Id {
			c.mapTasks[i].Status = req.Task.Status
			c.mapTasks[i].TmpFiles = req.Task.TmpFiles
		}
	}


	for i, task := range c.reduceTasks {
		if task.Id == req.Task.Id {
			c.reduceTasks[i].Status = req.Task.Status
			c.reduceTasks[i].OutputFile = req.Task.OutputFile
			break
		}
	}


	reply.Success = true
	return nil
}

func (c *Coordinator) GetId(req *GetIdReq, reply *GetIdResp) error {
	mu.Lock()
	defer mu.Unlock()
	c.workerIdCount ++
	reply.Id = c.workerIdCount
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mu = sync.Mutex{}
	c := Coordinator{
		fileNames: files,
		mapTasks: []Task{},
		reduceTasks: []Task{},
	}

	// Your code here.
	taskIds := make([]int, 0)
	for i:=0; i<len(files);i++ {
		taskIds = append(taskIds, i)
	}

	for i:=0;i<nReduce;i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			Id: i+100,
			Ttype: 1,
			TmpFiles: []string{},
			Status: 0,
			NReduce: nReduce,
			TaskIds: taskIds,
		})
	}
	fmt.Println("c.reduceTasks", len(c.reduceTasks), c.reduceTasks)

	for i:=0;i<len(files);i++ {
		oneTask := Task{
			Id: i,
			Ttype: 0,
			FileName: files[i],
			Content: "",
			TmpFiles: []string{},
			Status: 0,
			NReduce: nReduce,
			TaskIds: taskIds,
		}
		file, err := os.Open(files[i])
		if err != nil {
			log.Fatalf("cannot open %v", files[i])
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", files[i])
		}
		file.Close()
		oneTask.Content = string(content)
		c.mapTasks = append(c.mapTasks, oneTask)
	}
	fmt.Println("task assign finished.")
	c.server()
	return &c
}
