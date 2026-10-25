package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	var or func(channels ...<-chan struct{}) <-chan struct{}
	or = func(channels ...<-chan struct{}) <-chan struct{} {
		switch len(channels) {
		case 0:
			return nil
		case 1:
			return channels[0]
		}

		orDone := make(chan struct{})
		go func() {
			defer close(orDone)

			switch len(channels) {
			case 2:
				select {
				case <-channels[0]:
				case <-channels[1]:
				}
			default:
				select {
				case <-channels[0]:
				case <-channels[1]:
				case <-channels[2]:
				case <-or(append(channels[3:], orDone)...):
				}
			}
		}()
		return orDone
	}
	type startGoroutineFn func(
		done <-chan struct{},
		pulseInterval time.Duration,
	) (heartbeat <-chan struct{})

	newSteward := func(
		timeout time.Duration,
		startGoroutine startGoroutineFn,
	) startGoroutineFn {
		return func(
			done <-chan struct{},
			pulseInterval time.Duration,
		) <-chan struct{} {
			heartbeat := make(chan struct{})
			go func() {
				defer close(heartbeat)

				var wardDone chan struct{}
				var wardHeartbeat <-chan struct{}
				startWard := func() {
					wardDone = make(chan struct{})
					wardHeartbeat = startGoroutine(or(wardDone, done), timeout/2)
				}
				startWard()
				pulse := time.Tick(pulseInterval)

			monitorLoop:
				for {
					timeoutSignal := time.After(timeout)

					for {
						select {
						case <-pulse:
							select {
							case heartbeat <- struct{}{}:
							default:
							}
						case <-wardHeartbeat:
							continue monitorLoop
						case <-timeoutSignal:
							log.Println("steward: ward unhealthy; restarting")
							close(wardDone)
							startWard()
							continue monitorLoop
						case <-done:
							return
						}
					}
				}
			}()

			return heartbeat
		}
	}
	take := func(
		done <-chan struct{},
		valueStream <-chan struct{},
		num int,
	) <-chan struct{} {
		takeStream := make(chan struct{})
		go func() {
			defer close(takeStream)
			for i := 0; i < num; i++ {
				select {
				case <-done:
					return
				case takeStream <- <-valueStream:
				}
			}
		}()
		return takeStream
	}
	//bridge := func(
	//    done <-chan struct{},
	//    chanStream <-chan <-chan struct{},
	//) <-chan struct{} {
	//    valStream := make(chan struct{})
	//    go func() {
	//        defer close(valStream)
	//        for {
	//            var stream <-chan struct{}
	//            select {
	//            case maybeStream, ok := <-chanStream:
	//                if ok == false {
	//                    return
	//                }
	//                stream = maybeStream
	//            case <-done:
	//                return
	//            }
	//            for val := range orDone(done, stream) {
	//                select {
	//                case valStream <- val:
	//                case <-done:
	//                }
	//            }
	//        }
	//    }()
	//    return valStream
	//}
	doWorkFn := func(
		done <-chan struct{},
		intList ...int,
	) (startGoroutineFn, <-chan struct{}) {
		intChanStream := make(chan (<-chan struct{}))
		intStream := bridge(done, intChanStream)
		doWork := func(
			done <-chan struct{},
			pulseInterval time.Duration,
		) <-chan struct{} {
			intStream := make(chan struct{})
			heartbeat := make(chan struct{})
			go func() {
				defer close(intStream)
				select {
				case intChanStream <- intStream:
				case <-done:
					return
				}

				pulse := time.Tick(pulseInterval)

				for {
				valueLoop:
					for _, intVal := range intList {
						if intVal < 0 {
							log.Printf("negative value: %v\n", intVal)
							return
						}

						for {
							select {
							case <-pulse:
								select {
								case heartbeat <- struct{}{}:
								default:
								}
							case intStream <- intVal:
								continue valueLoop
							case <-done:
								return
							}
						}
					}
				}
			}()
			return heartbeat
		}
		return doWork, intStream
	}
	log.SetFlags(log.Ltime | log.LUTC)
	log.SetOutput(os.Stdout)

	done := make(chan struct{})
	defer close(done)

	doWork, intStream := doWorkFn(done, 1, 2, -1, 3, 4, 5)
	doWorkWithSteward := newSteward(1*time.Millisecond, doWork)
	doWorkWithSteward(done, 1*time.Hour)

	for intVal := range take(done, intStream, 6) {
		fmt.Printf("Received: %v\n", intVal)
	}
}
