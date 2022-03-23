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
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	reg_args := RegisterArgs{}
	reg_reply := RegisterReply{}
	reg_ok := false
	if !reg_ok {
		reg_ok = call("Coordinator.Register", &reg_args, &reg_reply)
	}
	worker_id := reg_reply.WorkerId
	n_map := reg_reply.M
	n_reduce := reg_reply.R
	for {
		ask_args := AskArgs{worker_id}
		ask_reply := AskReply{}
		ask_ok := call("Coordinator.AskForTask", &ask_args, &ask_reply)
		if !ask_ok {
			break
		}
		if ask_reply.Over {
			break
		}
		if ask_reply.TaskType == 0 { //Map
			filename := ask_reply.MapFileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			intermediate := mapf(filename, string(content))
			ofile := make([]*os.File, n_reduce)
			enc := make([]*json.Encoder, n_reduce)
			for i := 0; i < n_reduce; i++ {
				ofile[i], err = ioutil.TempFile("", fmt.Sprintf("mr-tmp-%d-*", worker_id))
				if err != nil {
					fmt.Println(err)
					log.Fatal("cannot open tempfile")
				}
				enc[i] = json.NewEncoder(ofile[i])
			}
			for _, kv := range intermediate {
				enc[ihash(kv.Key)%n_reduce].Encode(kv)
			}
			mid_file_name := fmt.Sprintf("mr-inter-%d-", ask_reply.TaskId)
			for i := 0; i < n_reduce; i++ {
				//ofile[i].Close() //For windows
				defer ofile[i].Close() //For Posix
				os.Rename(ofile[i].Name(), fmt.Sprintf("%v%d", mid_file_name, i))
			}

			finish_args := FinishMapArgs{
				worker_id,
				ask_reply.TaskId,
			}
			finish_reply := FinishMapReply{}
			finish_ok := call("Coordinator.FinishMap", &finish_args, &finish_reply)
			if !finish_ok {
				break
			}
		} else if ask_reply.TaskType == 1 { //Reduce
			intermediate := []KeyValue{}
			for i := 0; i < n_map; i++ {
				ifile, err := os.Open(fmt.Sprintf("mr-inter-%d-%d", i, ask_reply.TaskId))
				if err != nil {
					fmt.Println(err)
					log.Fatal("Open inter file error!")
				}
				dec := json.NewDecoder(ifile)
				kv := KeyValue{}
				for {
					err := dec.Decode(&kv)
					if err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))

			ofile, _ := ioutil.TempFile("", fmt.Sprintf("mr-out-%d-*", ask_reply.TaskId))
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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			oname := fmt.Sprintf("mr-out-%d", ask_reply.TaskId)
			// ofile.Close() //For Windows
			defer ofile.Close()
			os.Rename(ofile.Name(), oname)
			finish_args := FinishReduceArgs{
				WorkerId: worker_id,
				TaskId:   ask_reply.TaskId,
			}
			finish_reply := FinishReduceReply{}
			finishi_ok := call("Coordinator.FinishReduce", &finish_args, &finish_reply)
			if !finishi_ok {
				break
			}
			if finish_reply.Over {
				break
			}
		}
	}
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
