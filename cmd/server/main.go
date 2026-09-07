package main

import (
  "fmt"
  "sync"
)

func main() {
  var wg sync.WaitGroup

  ch := make(chan int)
  x, y, z := 1, 2, 3

  wg.Add(1)
  go receive(ch, &wg)

  ch <- x 
  ch <- y
  ch <- z
  close(ch)
  wg.Wait()
}

func receive(ch chan int, wg *sync.WaitGroup) {
  for v := range ch {
    fmt.Println(v)
  }

  wg.Done()
}
