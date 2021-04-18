package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

const (
	FilePerm 	= 0755
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type workerInfo struct {
	id 			int
	mapf 		func(string, string) []KeyValue
	reducef		func(string, []string) string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := workerInfo{}

	//Get an id from coordinator
	reply := RegisterWorkerReply{}
	call("Coordinator.RPCRegisterWorker", RegisterWorkerArgs{}, &reply)
	w.id = reply.Workerid
	w.mapf = mapf 
	w.reducef = reducef

	for {
		reply := AssignTaskReply{}
		b := call("Coordinator.RPCAssignTask", AssignTaskArgs{Workerid: w.id}, &reply)
		if b {
			if reply.TaskId == -1 {
				//No available task now
				time.Sleep(10 * time.Millisecond)
			}else{
				if reply.TaskType == TypeMap {
					//receive map task
					w.handleMap(reply.FileName, reply.TaskId, reply.ReducerCount)
				}else{
					//receive reduce task
					w.handleReduce(reply.MapCount, reply.TaskId)
				}

				reply := FinishTaskReply{}
				call("Coordinator.RPCFinishTask", FinishTaskArgs{Workerid: w.id}, &reply)
			}
		}else{
			//coordinator has exits
			break
		}
	}
}


func (w *workerInfo) handleMap(filename string, taskid int, nreduce int){
	content := readFile(filename)
	kvs := w.mapf(filename, content)
	m := make(map[int][]KeyValue)

	for i:=0; i<nreduce; i++ {
		m[i] = []KeyValue{}
	}

	for _, kv := range kvs {
		k := ihash(kv.Key) % nreduce
		m[k] = append(m[k], kv)
	}

	for k, v := range m {
		fn := "mr-"
		fn += strconv.Itoa(taskid)
		fn += "-"
		fn += strconv.Itoa(k)

		f, e := os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, FilePerm)
		if e != nil {
			log.Fatalf("cannot create file %v", fn)
		}
		enc := json.NewEncoder(f)
		for _, kv := range v {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		f.Close()
	}
}

func (w *workerInfo) handleReduce(nmap int, taskid int){
	
	var intermediate []KeyValue

	for i:=0; i<nmap; i++ {

		fn:= "mr-"
		fn+= strconv.Itoa(i)
		fn+= "-"
		fn+= strconv.Itoa(taskid)

		file, err := os.Open(fn)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-"
	oname += strconv.Itoa(taskid)
	ofile, _ := os.Create(oname)

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
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}


//readfile return all content of the file
func readFile(filename string) string{
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
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