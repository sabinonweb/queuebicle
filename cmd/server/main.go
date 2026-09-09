package main

import (
  "fmt"
  "sync"
)

type Job struct {
  ID int
  Value string
}

func NewJob(id int, value string) Job {
  return Job {
    ID: id,
    Value: value,
  }
}

func main() {
  var wg sync.WaitGroup

  ch := make(chan Job, 10)

  wg.Add(5)
  
  for i := 0; i < 5; i++ {
    go worker(ch, &wg, i)
  }
    
  j1 := NewJob(1, "hello")
  j2 := NewJob(2, "World")
  j3 := NewJob(3, "hora")
  j4 := NewJob(4, "hello")
  j5 := NewJob(5, "World")
  j6 := NewJob(6, "hora")
  j7 := NewJob(7, "hello")
  j8 := NewJob(8, "World")
  j9 := NewJob(9, "hora")
  j10 := NewJob(10, "hello")
  j11 := NewJob(11, "World")
  j12 := NewJob(12, "hora")




  jobs := []Job{j1, j2, j3, j4, j5, j6, j7, j8, j9, j10, j11, j12}
  
  for _, j := range jobs {
    submitJob(ch, j)
  }


  close(ch)
  wg.Wait()
}

func submitJob(ch chan Job, job Job) {
  ch <- job
}

func worker(ch chan Job, wg *sync.WaitGroup, n int) {
  for v := range ch {
    fmt.Println("From Worker", n, ":", v)
  }

  wg.Done()
}


