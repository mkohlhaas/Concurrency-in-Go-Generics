package main

import (
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func BenchmarkUnbufferedWrite(b *testing.B) {
	performWrite(b, tmpFileOrFatal())
}

func BenchmarkBufferedWrite(b *testing.B) {
	bufferredFile := bufio.NewWriter(tmpFileOrFatal())
	performWrite(b, bufio.NewWriter(bufferredFile))
}

func tmpFileOrFatal() *os.File {
	file, err := ioutil.TempFile("", "tmp")
	if err != nil {
		log.Fatal("error: %v", err)
	}
	return file
}

func performWrite(b *testing.B, writer io.Writer) {
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

	done := make(chan struct{})
	defer close(done)

	b.ResetTimer()
	for bt := range take(done, repeat(done, byte(0)), b.N) {
		writer.Write([]byte{bt})
	}
}
