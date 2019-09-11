package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

type KVSlice []KeyValue

func (kv KVSlice) Len() int {
	return len(kv)
}

func (kv KVSlice) Less(i, j int) bool {
	return kv[i].Key < kv[j].Key
}
func (kv KVSlice) Swap(i, j int) {
	kv[i], kv[j] = kv[j], kv[i]
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int,       // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	keyVaues := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		decoder := json.NewDecoder(file)
		for decoder.More() {
			kvStruct := KeyValue{}
			decoder.Decode(&kvStruct)
			keyVaues = append(keyVaues, kvStruct)
		}
	}
	if len(keyVaues) == 0 {
		return
	}
	sort.Sort(KVSlice(keyVaues))
	file, _ := os.Create(outFile)
	encoder := json.NewEncoder(file)
	defer file.Close()
	curIndex := 0
	curValues := []string{keyVaues[0].Value}
	for i := 1; i < len(keyVaues); i++ {
		kv := keyVaues[i]
		if kv.Key != keyVaues[curIndex].Key {
			reduceValue := reduceF(keyVaues[curIndex].Key, curValues)
			encoder.Encode(KeyValue{keyVaues[curIndex].Key, reduceValue})
			curIndex = i
			curValues = []string{kv.Value}
		} else {
			curValues = append(curValues, kv.Value)
		}
	}
	reduceValue := reduceF(keyVaues[curIndex].Key, curValues)
	encoder.Encode(KeyValue{keyVaues[curIndex].Key, reduceValue})
}
