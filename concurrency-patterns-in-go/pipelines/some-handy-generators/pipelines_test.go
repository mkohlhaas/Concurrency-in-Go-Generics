package pipelines

import (
	"fmt"
	"testing"
)

func BenchmarkGeneric(b *testing.B) {
	repeat := func[T any](
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
	take := func[T any](
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
	toString := func[T fmt.Stringer](
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
				case stringStream <- v.String():
				}
			}
		}()
		return stringStream
	}
	done := make(chan struct{})
	defer close(done)

	b.ResetTimer()
	for range toString(done, take(done, repeat(done, stringer("a")), b.N)) {
	}
}

type stringer string

func (s stringer) String() string { return string(s) }

func BenchmarkTyped(b *testing.B) {
	repeat := func[T any](
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

	take := func[T any](
		done <-chan struct{},
		valueStream <-chan T,
		num int,
	) <-chan T {
		takeStream := make(chan T)
		go func() {
			defer close(takeStream)
			for i := num; i > 0 || i == -1; {
				if i != -1 {
					i--
				}
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

	done := make(chan struct{})
	defer close(done)

	b.ResetTimer()
	for range take(done, repeat(done, "a"), b.N) {
	}
}
