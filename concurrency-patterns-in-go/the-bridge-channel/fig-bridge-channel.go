package main

import (
	"fmt"
)

func orDone[T any](done <-chan struct{}, c <-chan T) <-chan T {
	valStream := make(chan T)
	go func() {
		defer close(valStream)
		for {
			select {
			case <-done:
				return
			case v, ok := <-c:
				if !ok {
					return
				}
				select {
				case valStream <- v:
				case <-done:
					return
				}
			}
		}
	}()
	return valStream
}

func bridge[T any](
	done <-chan struct{},
	chanStream <-chan <-chan T,
) <-chan T {
	valStream := make(chan T)
	go func() {
		defer close(valStream)
		for {
			var stream <-chan T
			select {
			case maybeStream, ok := <-chanStream:
				if !ok {
					return
				}
				stream = maybeStream
			case <-done:
				return
			}
			for val := range orDone(done, stream) {
				select {
				case valStream <- val:
				case <-done:
					return
				}
			}
		}
	}()
	return valStream
}

func main() {
	genVals := func() <-chan <-chan int {
		chanStream := make(chan (<-chan int))
		go func() {
			defer close(chanStream)
			for i := 0; i < 10; i++ {
				stream := make(chan int, 1)
				stream <- i
				close(stream)
				chanStream <- stream
			}
		}()
		return chanStream
	}

	done := make(chan struct{})
	defer close(done)

	for v := range bridge(done, genVals()) {
		fmt.Printf("%v ", v)
	}
}
