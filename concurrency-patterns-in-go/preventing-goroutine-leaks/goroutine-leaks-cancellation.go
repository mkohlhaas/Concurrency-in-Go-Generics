package main

import (
	"fmt"
	"time"
)

func main() {
	doWork := func(
		done <-chan struct{},
		strings <-chan string,
	) <-chan struct{} { // <1>
		terminated := make(chan struct{})
		go func() {
			defer fmt.Println("doWork exited.")
			defer close(terminated)
			for {
				select {
				case s := <-strings:
					// Do something interesting
					fmt.Println(s)
				case <-done: // <2>
					return
				}
			}
		}()
		return terminated
	}

	done := make(chan struct{})
	terminated := doWork(done, nil)

	go func() { // <3>
		// Cancel the operation after 1 second.
		time.Sleep(1 * time.Second)
		fmt.Println("Canceling doWork goroutine...")
		close(done)
	}()

	<-terminated // <4>
	fmt.Println("Done.")
}
