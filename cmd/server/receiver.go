package main

import (
  "fmt"
  "sync"
)

func receive(wg *sync.WaitGroup, shared chan Job) {
  fmt.Println("starting workers")
  for j := range shared {
    fmt.Println("Job", j)
  }

  wg.Done()
}

