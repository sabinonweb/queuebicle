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

  ch := make(chan Job)

  wg.Add(5)
  
  for i := 0; i < 5; i++ {
    go worker(ch, &wg, i)
  } 
  
  j1 := NewJob(1, "hello")
  j2 := NewJob(2, "World")
  j3 := NewJob(3, "hora")

  jobs := []Job{j1, j2, j3}
  
  for _, j := range jobs {
    submit_job(ch, j)
  } 

  close(ch)
  wg.Wait()
}

func submit_job(ch chan Job, job Job) {
  ch <- job
}

func worker(ch chan Job, wg *sync.WaitGroup, n int) {
  for v := range ch {
    fmt.Println("From Worker", n, ":", v)
  }

  wg.Done()
}


