package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type KVS []KeyValue

func (kvs KVS) Len() int           { return len(kvs) }
func (kvs KVS) Swap(i, j int)      { kvs[i], kvs[j] = kvs[j], kvs[i] }
func (kvs KVS) Less(i, j int) bool { return strings.Compare(kvs[i].Key, kvs[j].Key) < 0 }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvs := []KeyValue{}

	i := 0

	for i < nMap {

		f, err := os.Open(reduceName(jobName, i, reduceTask))
		fmt.Println(reduceName(jobName, i, reduceTask))
		defer f.Close()

		if err != nil {
			fmt.Println("open error")
			fmt.Println(err)
			return
		}

		dec := json.NewDecoder(f)

		for {
			var kv KeyValue

			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				fmt.Println("decode error")

				return
			}
			kvs = append(kvs, kv)
		}
		i++
	}

	fmt.Println(len(kvs))

	sort.Sort(KVS(kvs))

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

	fout, err := os.Create(outFile)
	defer fout.Close()
	if err != nil {
		return
	}
	enc := json.NewEncoder(fout)

	for _, one := range kvs {
		enc.Encode(KeyValue{one.Key, reduceF(one.Key, []string{one.Value})})
	}
}
