package main

import (
	"fmt"
	"math/rand"
	"time"
)

func repeatFn[T any](
	done <-chan struct{},
	fn func() T,
) <-chan T {
	valueStream := make(chan T)
	go func() {
		defer close(valueStream)
		for {
			select {
			case <-done:
				return
			case valueStream <- fn():
			}
		}
	}()
	return valueStream
}

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

func primeFinder(done <-chan struct{}, intStream <-chan int) <-chan int {
	primeStream := make(chan int)
	go func() {
		defer close(primeStream)
		for integer := range intStream {
			n := integer - 1
			if n < 2 {
				continue
			}
			prime := true
			for divisor := n - 1; divisor > 1; divisor-- {
				if n%divisor == 0 {
					prime = false
					break
				}
			}

			if prime {
				select {
				case <-done:
					return
				case primeStream <- n:
				}
			}
		}
	}()
	return primeStream
}

func main() {
	randFn := func() int { return rand.Intn(50000000) }

	done := make(chan struct{})
	defer close(done)

	start := time.Now()

	randIntStream := repeatFn(done, randFn)
	fmt.Println("Primes:")
	for prime := range take(done, primeFinder(done, randIntStream), 10) {
		fmt.Printf("\t%d\n", prime)
	}

	fmt.Printf("Search took: %v", time.Since(start))
}
