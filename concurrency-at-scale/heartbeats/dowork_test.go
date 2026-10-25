package main

import (
	"testing"
	"time"
)

func DoWork(
	done <-chan struct{},
	nums ...int,
) (<-chan struct{}, <-chan int) {
	heartbeat := make(chan struct{}, 1)
	intStream := make(chan int)
	go func() {
		defer close(heartbeat)
		defer close(intStream)

		time.Sleep(2 * time.Second)

		for _, n := range nums {
			select {
			case heartbeat <- struct{}{}:
			default:
			}

			select {
			case <-done:
				return
			case intStream <- n:
			}
		}
	}()

	return heartbeat, intStream
}
