package raft

import "time"
import "fmt"


func main() {
	// Begin to collect response from followers
	timeChan := time.NewTimer(time.Millisecond * time.Duration(1))
	since := time.Now()
	for  {
		select 	{
		case <-timeChan.C:
			fmt.Printf("Raft Server %[1]d has statrted sending heart beat messages after %[2]d milli seconds",1,time.Now().Sub(since))
			fmt.Println()
			for i := 0; i < 10; i++ {

			}
			since = time.Now()
			timeChan = time.NewTimer(time.Millisecond * time.Duration(150))

		}
	}
}
func Test(){
	main()
}
