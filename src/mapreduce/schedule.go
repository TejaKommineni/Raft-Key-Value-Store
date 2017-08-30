package mapreduce

import "fmt"
import "sync"

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	fmt.Println("I entered schedule")
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	var wg sync.WaitGroup
	switch phase {
	case mapPhase:	
		ntasks = len(mapFiles)
		n_other = nReduce
		//fmt.Println("number of tasks are", ntasks)
		for i:=0;i<ntasks;i++{
			worker := <-registerChan
			fmt.Println("I am worker ",worker)
			wg.Add(1)
			dotasks := DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
			go func(){
				call(worker, "Worker.DoTask", dotasks, nil)
				wg.Done()
				registerChan <- worker
				//fmt.Println("My Work here is done",worker)
			}()
		
		}
		//fmt.Println("I am out of loop")		
		wg.Wait()
		//fmt.Println("I am not waiting anymore")
			
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		for i:=0;i<ntasks;i++{
                        worker := <-registerChan
                        wg.Add(1)
                        dotasks := DoTaskArgs{jobName, "", phase, i, n_other}
                        go func(){
                                call(worker, "Worker.DoTask", dotasks, nil)
                                wg.Done()
				registerChan <- worker
                        }()
                }
                wg.Wait()
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
