package mr

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"



//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func handleMapTask(task *Task, mapf func(string, string) []KeyValue) {
	mapRes := mapf(task.FileName, task.Content)
	files := make([]string, 0)
	for _, kv := range mapRes {
		filename := fmt.Sprintf("mr-tmp-%v-%v", task.Id, ihash(kv.Key) % task.NReduce)
		f, _ := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		f.WriteString(kv.Key+" "+kv.Value + "\n")
		f.Close()
		files = append(files, filename)
	}

	task.TmpFiles = files
	task.Status = 1
	reportTask(task)
}

func handleReduceTask(task *Task, reducef func(string, []string) string) {
	outFile := fmt.Sprintf("mr-out-%v", task.Id-100)
	f, _ := os.OpenFile(outFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	defer f.Close()
	intermediate := []KeyValue{}
	for _, j := range task.TaskIds {
		filename := fmt.Sprintf("mr-tmp-%v-%v", j, task.Id-100)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			arr := strings.SplitN(line, " ", 2)
			intermediate = append(intermediate, KeyValue{arr[0], arr[1]})
		}
	}


	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		f.WriteString(intermediate[i].Key + " " + output + "\n")
		i = j
	}

	task.OutputFile = outFile
	task.Status = 1
	reportTask(task)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {


	// Your worker implementation here.
	workerId := os.Getuid()
	for {
		task := askForTask(workerId)
		if task == nil {
			return
		}

		if task.Ttype == 0 {
			handleMapTask(task, mapf)
		} else if task.Ttype == 1 {
			handleReduceTask(task, reducef)
		} else if task.Ttype == 2 {
			time.Sleep(3*time.Second)
		}
	}
}

func askForTask(workerId int) *Task {
	args := TaskReq{
		WorkerId: workerId,
	}
	var reply TaskResp
	call("Coordinator.AssignTask", &args, &reply)
	return reply.Task
}

func reportTask(task *Task) bool {
	args := ReportReq{
		Task: task,
	}
	var reply ReportResp
	call("Coordinator.ReportTask", &args, &reply)
	return reply.Success
}

func getId() int {
	args := GetIdReq{}
	var reply GetIdResp
	call("Coordinator.GetId", &args, &reply)
	return reply.Id
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
