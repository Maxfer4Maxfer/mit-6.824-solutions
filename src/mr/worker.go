package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	timeoutAfterFailure = 10 * time.Second
	hashMask            = uint32(0x7fffffff)
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

	return int(h.Sum32() & hashMask)
}

func outcomeMapFileName(mapTaskID string, reduceTaskNumber int) string {
	return fmt.Sprintf("mr-%s-%d.txt", mapTaskID, reduceTaskNumber)
}

func incomeReduceTemplate(reduceTaskNumber TaskID) string {
	return fmt.Sprintf("mr-[0-9]-%s.txt", reduceTaskNumber)
}

func outcameReduceFileName(reduceTaskNumber TaskID) string {
	return fmt.Sprintf("mr-out-%s.txt", reduceTaskNumber)
}

func randomName() string {
	r := rand.New(rand.NewSource(99))

	return fmt.Sprintf("worker-%d", r.Int31())
}

//
// main/mrworker.go calls this function.
//
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	var (
		workerName = randomName()
		log        = log.New(os.Stdout, workerName+" | ", log.Lshortfile)
	)

	log.Printf("Worker %s is starting his duty", workerName)

	// Your worker implementation here.
	for {
		// Ask the Coordinator for a job
		var (
			ctwReply CommitToWorkReply
			ctwArgs  CommitToWorkArgs
		)

		ctwArgs.Worker = workerName

		if ok := call(log, "Coordinator.CommitToWork", &ctwArgs, &ctwReply); !ok {
			log.Println("call failed!")
			time.Sleep(timeoutAfterFailure)
			continue
		}

		if ctwReply.EverythingIsDone {
			log.Printf("Everything is Done. Nothing more to do")
			break
		}

		if ctwReply.ComeBackAfter != 0 {
			time.Sleep(ctwReply.ComeBackAfter)
			continue
		}

		// Do job
		rrArgs, err := doJob(log, ctwReply, workerName, mapf, reducef)
		if err != nil {
			log.Printf("Error during executing job %s", err)
			time.Sleep(timeoutAfterFailure)
			continue
		}

		// Notify the job has been done
		var rrReply RegisterResultReply

		if ok := call(log, "Coordinator.RegisterResult", &rrArgs, &rrReply); !ok {
			log.Println("call failed!")
			time.Sleep(timeoutAfterFailure)
			continue
		}

		if rrReply.EverythingIsDone {
			log.Printf("Everything is Done. Nothing more to do")
			break
		}
	}

	log.Printf("Shuting down....")
}

func doJob(
	log *log.Logger,
	ctwReply CommitToWorkReply,
	workerName string,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) (*RegisterResultArgs, error) {
	switch ctwReply.TaskType {
	case MapTaskType:
		err := doMapJob(log, ctwReply.ID, ctwReply.File, ctwReply.NReduce, mapf)
		if err != nil {
			return nil, err
		}
	case ReduceTaskType:
		err := doReduceJob(log, ctwReply.ID, reducef)
		if err != nil {
			return nil, err
		}
	}

	return &RegisterResultArgs{
		Worker:   workerName,
		TaskType: ctwReply.TaskType,
		ID:       ctwReply.ID,
	}, nil
}

func doMapJob(
	log *log.Logger,
	taskID TaskID,
	inputFileName string,
	nReduce int,
	mapf func(string, string) []KeyValue,
) error {
	log.Printf("Got map job to do ID: %d File: %s", taskID, inputFileName)

	file, err := os.Open(inputFileName)
	if err != nil {
		return fmt.Errorf("failure open file %s: %w", inputFileName, err)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failure read from file %s: %w", file.Name(), err)
	}

	log.Printf("Read %d bytes from file %s", len(content), inputFileName)

	if err := file.Close(); err != nil {
		return fmt.Errorf("failure close file %s: %w", file.Name(), err)
	}

	log.Printf("Starting execute the given map function")

	kvs := mapf(inputFileName, string(content))

	log.Printf("Got %d KeyValue pairs", len(kvs))

	if err := writeMapResult(log, kvs, taskID, nReduce); err != nil {
		return err
	}

	return nil
}

func writeMapResult(
	log *log.Logger,
	kvs []KeyValue,
	taskID TaskID,
	nReduce int,
) error {
	gkvs := make(map[string][]KeyValue)

	for _, kv := range kvs {
		fname := outcomeMapFileName(taskID.String(), ihash(kv.Key)%nReduce)

		gkvs[fname] = append(gkvs[fname], kv)
	}

	for fname, kvs := range gkvs {
		log.Printf("Write to %s file", fname)

		file, err := os.Create(fname)
		if err != nil {
			return fmt.Errorf("failure create file %s: %w", fname, err)
		}

		enc := json.NewEncoder(file)

		for i := range kvs {
			kv := kvs[i]
			if err := enc.Encode(&kv); err != nil {
				return fmt.Errorf("failure encode kv pair %+v: %w", kv, err)
			}
		}

		if err := file.Close(); err != nil {
			return fmt.Errorf("failure close file %s: %w", file.Name(), err)
		}
	}

	return nil
}

func doReduceJob(
	log *log.Logger,
	taskID TaskID,
	reducef func(string, []string) string,
) error {
	log.Printf("Got reduce job to do ID: %d", taskID)

	template := incomeReduceTemplate(taskID)

	fnames, err := filepath.Glob(template)
	if err != nil {
		return fmt.Errorf("failure open files for template %s: %w", template, err)
	}

	valuesByKey, err := readValuesFromFiles(log, fnames)
	if err != nil {
		return fmt.Errorf("failure read kv pairs from files: %w", err)
	}

	reduceInput := make([]KeyValue, 0, len(valuesByKey))
	for k, vs := range valuesByKey {
		reduceInput = append(reduceInput, KeyValue{
			Key: k, Value: reducef(k, vs),
		})
	}

	sort.Slice(reduceInput,
		func(i, j int) bool { return reduceInput[i].Key < reduceInput[j].Key })

	fname := outcameReduceFileName(taskID)

	file, err := os.Create(fname)
	if err != nil {
		return fmt.Errorf("failure create file %s: %w", fname, err)
	}

	log.Printf("Writing to %s file", outcameReduceFileName(taskID))

	for _, kv := range reduceInput {
		if _, err := fmt.Fprintf(file, "%s %s\n", kv.Key, kv.Value); err != nil {
			return fmt.Errorf("failure write to file %s: %w", fname, err)
		}
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("failure close file %s: %w", file.Name(), err)
	}

	return nil
}

func readValuesFromFiles(
	log *log.Logger,
	fnames []string,
) (map[string][]string, error) {
	valuesByKey := make(map[string][]string)

	for _, fname := range fnames {
		log.Printf("Read from %s file", fname)

		file, err := os.Open(fname)
		if err != nil {
			return nil, fmt.Errorf("failure to open file %s: %w", fname, err)
		}

		var (
			kv    KeyValue
			count int
			dec   = json.NewDecoder(file)
		)

		for dec.More() {
			err = dec.Decode(&kv)
			if err != nil {
				return nil, fmt.Errorf("failure decode kv valie %+v: %w", kv, err)
			}

			if kv.Key == "ABOUT" {
				log.Printf("%+v", kv)
			}

			valuesByKey[kv.Key] = append(valuesByKey[kv.Key], kv.Value)
			count++
		}

		log.Printf("Read %d key value pairs", count)

		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("failure close file %s: %w", fname, err)
		}
	}

	return valuesByKey, nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(
	log *log.Logger,
	rpcname string,
	args interface{},
	reply interface{},
) bool {
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

	log.Printf("error call %s: %d", rpcname, err)

	return false
}
