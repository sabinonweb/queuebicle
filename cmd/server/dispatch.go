package main

import (
	"fmt"
	"sync"
)

func dispatcher(queues []chan Job, shared chan Job, wg *sync.WaitGroup) {
  fmt.Println("starting dispatcher")
  i := 0

  done := make([]bool, len(queues))
  outer:
  for {
    select {
    case j, ok := <- queues[i]:
      if ok {
        shared <- j
      } else {
        done[i] = true 
      }

      default:
    }
    
    allDone := true
    for _, value := range done {
      allDone = allDone && value
    }

    if allDone {
      break outer
    }

    i = (i + 1) % len(queues)
  }

  close(shared)
  wg.Done()
}
