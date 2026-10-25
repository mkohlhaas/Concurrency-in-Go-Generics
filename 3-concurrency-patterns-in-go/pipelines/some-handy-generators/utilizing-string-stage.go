package main

import (
	"fmt"
)

func take[T any](
	done <-chan struct{},
	valueStream <-chan T,
	num int,
) <-chan T {
	takeStream := make(chan T)
	go func() {
		defer close(takeStream)
		for i := 0; i < num; i++ {
			select {
			case <-done:
				return
			case v, ok := <-valueStream:
				if !ok {
					return
				}
				select {
				case <-done:
					return
				case takeStream <- v:
				}
			}
		}
	}()
	return takeStream
}

func repeat[T any](
	done <-chan struct{},
	values ...T,
) <-chan T {
	valueStream := make(chan T)
	go func() {
		defer close(valueStream)
		for {
			for _, v := range values {
				select {
				case <-done:
					return
				case valueStream <- v:
				}
			}
		}
	}()
	return valueStream
}

func toString[T any](
	done <-chan struct{},
	valueStream <-chan T,
) <-chan string {
	stringStream := make(chan string)
	go func() {
		defer close(stringStream)
		for v := range valueStream {
			select {
			case <-done:
				return
			case stringStream <- fmt.Sprint(v):
			}
		}
	}()
	return stringStream
}

func main() {
	done := make(chan struct{})
	defer close(done)

	var message string
	for token := range toString(done, take(done, repeat(done, "I", "am."), 5)) {
		message += token
	}

	fmt.Printf("message: %s...", message)
}
