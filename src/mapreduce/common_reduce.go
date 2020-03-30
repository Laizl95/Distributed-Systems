package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
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
	keyValues := make(map[string][]string)
	//从nmap个服务器中取出当前复制的reduce文件
	for i := 0; i < nMap; i++ {
		file,err := os.Open(reduceName(jobName, i, reduceTask))
		if err != nil {
			fmt.Printf("reduce file:%s can't open\n",reduceName(jobName, i, reduceTask))
		} else {
			enc := json.NewDecoder(file)
			for {
				var kv KeyValue
				err := enc.Decode(&kv)
				if err != nil {
					break  // 此时文件解码完毕
				}
				_, ok := keyValues[kv.Key]
				if !ok { // 说明当前并没有这个key
					keyValues[kv.Key] = make([]string, 0)
				}
				keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
			}
			file.Close()
		}
	}
	var keys []string
	for k, _ := range keyValues {
		keys = append(keys, k)
	}
	//sort.Strings(keys)  // 递增排序
	file,err := os.Create(mergeName(jobName, reduceTask))
	if err != nil {
		fmt.Printf("reduce merge file:%s can't open\n",mergeName(jobName, reduceTask))
		return
	}
	enc := json.NewEncoder(file)
	for _,k := range keys {
		enc.Encode(KeyValue{k, reduceF(k,keyValues[k])})
	}
	file.Close()
}
