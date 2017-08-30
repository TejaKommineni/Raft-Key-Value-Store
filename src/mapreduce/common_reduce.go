package mapreduce

import (
        "encoding/json"
	"os"
	//"io"
	"fmt"
	"sort"	
	"strings"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.

type Element struct {
	Key  string
	Value  []string
}
type ByKey []Element

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	var mapValues[]Element
	for i:=0;i<nMap;i++{
		interFile := reduceName(jobName, i, reduceTaskNumber)
		f, _ := os.OpenFile(interFile, os.O_RDWR|os.O_CREATE, 0755)
		dec := json.NewDecoder(f)
		k:=0		
		for dec.More(){
			var m KeyValue
			if err := dec.Decode(&m); err != nil {
				break
			}
					

			flag := false
			for j:=0;j<len(mapValues);j++{
				if mapValues[j].Key ==  m.Key {
				if  strings.Compare(mapValues[j].Key,"was") == 0 {
				//	fmt.Println(k, mapValues[j].Key, m.Key)
					k= k+1		
				}	
				flag = true	
				value := mapValues[j].Value
        	                value = append(value , m.Value)
	                        mapValues[j].Value = value
				break
				}
			
			}


			if flag != true{
				temp1 := []string{m.Value}
				temp := Element{m.Key,temp1}
				mapValues = append(mapValues ,temp)
			}	
		//fmt.Println(k, m.Key)
		
		}
		 fmt.Println("was values", k)

		f.Close()
		fmt.Println("job",i,"task",reduceTaskNumber)
	} 
	sort.Sort(ByKey(mapValues))       
	//var keys []int
	//for k := range mapValues {
	//num, _ := strconv.Atoi(k)
        //keys = append(keys,num)
    	//}	
	//sort.Ints(keys)
	fmt.Println("teja", len(mapValues))
	f, _ := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE, 0755)
	enc := json.NewEncoder(f)	
	for _ ,elem:= range mapValues{
		enc.Encode(KeyValue{elem.Key, reduceF(elem.Key, elem.Value)})	
	}
	f.Close()
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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
}
