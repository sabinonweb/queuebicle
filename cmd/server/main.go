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

	for i := 0; i < 5; i++ {
		go receive(&wg, ch1, ch2)

	}

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

func receive(wg *sync.WaitGroup, ch1 chan Job, ch2 chan Job) {
  ch1Ok := false
	ch2Ok := false

outer:
	for {
		select {
		  case v, ok := <-ch1:
			  if ok {
				  fmt.Println(v)
			  } else {
				  ch1Ok = true
			  }

		  case v, ok := <-ch2:
			  if ok {
				  fmt.Println(v)
			  } else {
				  ch2Ok = true
			  }
		}

    if ch1Ok && ch2Ok {
				break outer
			}
	}
	wg.Done()
}
