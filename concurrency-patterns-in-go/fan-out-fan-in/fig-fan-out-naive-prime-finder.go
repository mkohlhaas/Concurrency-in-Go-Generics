package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
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

func fanIn[T any](
	done <-chan struct{},
	channels ...<-chan T,
) <-chan T {
	var wg sync.WaitGroup
	multiplexedStream := make(chan T)

	multiplex := func(c <-chan T) {
		defer wg.Done()
		for i := range c {
			select {
			case <-done:
				return
			case multiplexedStream <- i:
			}
		}
	}

	wg.Add(len(channels))
	for _, c := range channels {
		go multiplex(c)
	}

	go func() {
		wg.Wait()
		close(multiplexedStream)
	}()

	return multiplexedStream
}

func main() {

	done := make(chan struct{})
	defer close(done)

	start := time.Now()

	randFn := func() int { return rand.Intn(50000000) }

	randIntStream := repeatFn(done, randFn)

	numFinders := runtime.NumCPU()
	fmt.Printf("Spinning up %d prime finders.\n", numFinders)
	finders := make([]<-chan int, numFinders)
	fmt.Println("Primes:")
	for i := 0; i < numFinders; i++ {
		finders[i] = primeFinder(done, randIntStream)
	}

	for prime := range take(done, fanIn(done, finders...), 10) {
		fmt.Printf("\t%d\n", prime)
	}

	fmt.Printf("Search took: %v", time.Since(start))
}
