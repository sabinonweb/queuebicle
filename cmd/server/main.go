package main

import (
	"fmt"
	"sync"
)

func main() {
	var wg sync.WaitGroup

	ch1 := make(chan Job)
	ch2 := make(chan Job)

	wg.Add(5)

	jobs1 := []Job{
		newJob("job-1", "alpha"),
		newJob("job-2", "beta"),
		newJob("job-3", "gamma"),
		newJob("job-4", "delta"),
		newJob("job-5", "epsilon"),
		newJob("job-6", "zeta"),
		newJob("job-7", "eta"),
		newJob("job-8", "theta"),
		newJob("job-9", "iota"),
		newJob("job-10", "kappa"),
	}

	jobs2 := []Job{
		newJob("job-11", "alpha"),
		newJob("job-12", "beta"),
		newJob("job-13", "gamma"),
		newJob("job-14", "delta"),
		newJob("job-15", "epsilon"),
		newJob("job-16", "zeta"),
		newJob("job-17", "eta"),
		newJob("job-18", "theta"),
		newJob("job-19", "iota"),
		newJob("job-20", "kappa"),
	}

	tenant1 := newTenant("tenant-a", ch1)
	tenant2 := newTenant("tenant-b", ch2)
  
  tenants := []chan Job {
    tenant1.queue, tenant2.queue,
  }

  shared := make(chan Job)

  for i := 0; i < 5; i++ {
		go receive(&wg, shared)
	}

  go dispatcher(tenants, shared, &wg)

	for _, job := range jobs1 {
		tenant1.enqueue(job)
	}

	close(ch1)

	for _, job := range jobs2 {
		tenant2.enqueue(job)
	}

	close(ch2) 

	wg.Wait()
}

func receive(wg *sync.WaitGroup, shared chan Job) {
  fmt.Println("starting workers")
  for j := range shared {
    fmt.Println("Job", j)
  }

  wg.Done()
}

