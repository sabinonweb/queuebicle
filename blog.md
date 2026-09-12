```
Problem: We have multiple tenants which have their own queues but only a limited number of workers. We need to find a way to treat each tenant fairly.
```

Let's start with the most basic version where a channel send the value and the receiver prints it.

~~~go
func main() {
    ch := make(chan int)
    x, y, z := 1, 2, 3
    ch <- x
    ch <- y
    ch <- z
    for v := range ch {
        fmt.Println(v)
    }
}
~~~


This was the basic code that I wrote and started off with. 
`ch := make(chan int)` creates an unbuffered channel of int type. `x, y, z` are the jobs to be sent. They are then sent using `ch` . 

### Issue #1
But there is an issue here. First of all, everything is running in a single thread/goroutine only. Since, it is an unbuffered channel, the receiver has to receive it immediately. Also, the part which receives the data can never run because it is stuck in the send itself. So, it never gets a chance to run.

### Fix #1

~~~go
func receive(ch chan int) {
    for v := range ch {
        fmt.Println(v)
    }
}
~~~

~~~go
func main() {
  ch := make(chan int)

  x, y, z := 1, 2, 3

  go receive(ch)

  ch <- x
  ch <- y
  ch <- z
}

func receive(ch chan int) {
    for v := range ch {
        fmt.Println(v)
    }
}
~~~

If we run call `go receive(ch)` after the `ch <- x ch <- y ch <- z` call, it would still be a deadlock(same reasoning as before).

### Issue #2

![[Screenshot 2026-09-12 at 08.59.39.png]]

But there is another problem. `main()` exits immediately after the last send without waiting for the `receive` to finish which only prints `1 2` and never lets `3` to be printed. 

### Fix #2

Add a `waitgroup` .

~~~go
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
  go receive(&wg, ch)

  ch <- x
  ch <- y
  ch <- z

  wg.Wait()
}

func receive(wg *sync.WaitGroup, ch chan int) {
    for v := range ch {
        fmt.Println(v)
    }

    wg.Done()
}
~~~

`wg.Add(1)` indicates there is one `goroutine` to be waited for. `wg.Wait()` asks the `main/caller` to wait for it. `wg.Done()` in the `receive` `goroutine` indicates that the `goroutine` has finished running and `wg` decrements the number in the `wg.Add(n)` .
It prints all 3 values but creates a new error.

### Issue #3

![[Screenshot 2026-09-12 at 09.11.02.png]]

The problem occurred because the `receive` loop kept on waiting for the next value to arrive and `main` waited for the `receive` to call `Done` causing a Deadlock. 

### Fix #3

We need a mechanism to let the receive know that there are no more values arriving and that mechanism is `close(ch)`

~~~go
ch <- x
ch <- y
ch <- z
close(ch)
~~~

#### Interesting test

~~~go
func main() {
  ch := make(chan int, 5)

  x, y, z := 1, 2, 3

  ch <- x
  ch <- y
  ch <- z

  for v := range ch {
    fmt.Println(v)
  }
}
~~~

When I added a buffered channel, it did print 3 values but kept on waiting for another value and deadlock was caused.

![[Screenshot 2026-09-12 at 08.49.59.png]]

#### Fix

~~~go
func main() {
  ch := make(chan int, 5)

  x, y, z := 1, 2, 3

  ch <- x
  ch <- y
  ch <- z
  close(ch)

  for v := range ch {
    fmt.Println(v)
  }
}
~~~

If we add close(ch) after the values, it says `no more values are coming` and it closes the channel.

## Adding Job and Tenant

Add two files `job.go` and `tenant.go`. We will figure it out if we need them later on.

~~~go
// job.go
package main

type JobID string

type Job struct {
  id JobID
  value string
}

func newJob(id JobID, value string) Job {
  return Job {
    id, 
    value,
  }
}
~~~

~~~go
// tenant.go
package main

type TenantID string

type Tenant struct {
  id TenantID
  queue chan Job
}

func newTenant(id TenantID, queue chan Job) Tenant {
  return Tenant {
    id,
    queue,
  }
}
~~~

~~~go
func main() {
  var wg sync.WaitGroup

  ch := make(chan Job)
  wg.Add(5)

  for i := 0; i < 5; i++ {
    go receive(&wg, ch)

  }
  
  jobs := []Job{
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

  tenant := newTenant("tenant-a", ch)

  for _, job := range jobs {
    tenant.enqueue(job)
  } 
  
  close(ch)

  wg.Wait()
}

func receive(wg *sync.WaitGroup, ch chan Job) {
    for v := range ch {
        fmt.Println(v)
    }

    wg.Done()
}
~~~

This creates 10 jobs and `enqueues` it to the tenant. `5` workers are created to complete these jobs.

```
> go run .
{job-1 alpha}
{job-4 delta}
{job-2 beta}
{job-3 gamma}
{job-5 epsilon}
{job-10 kappa}
{job-6 zeta}
{job-7 eta}
{job-9 iota}
{job-8 theta}
```

```
Socration:
Your 5 workers right now are each running for v := range ch on one specific channel you hand them (receive(&wg, ch) — always that same ch). If you create tenant-b with a completely new, separate channel and enqueue jobs onto that channel instead of tenant-a's — will your existing 5 workers ever see those jobs? Why or why not, given what receive actually does?
```

```
My answer: Not for now until and unless we explicitely send them the designated channels for each tenant.
```

Let's add one more tenant and see how it fares.

~~~go
func main() {
  var wg sync.WaitGroup

  ch1 := make(chan Job)
  ch2 := make(chan Job)

  wg.Add(5)

  for i := 0; i < 5; i++ {
    go receive(&wg, ch1)

  }
  
  jobs := []Job{
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

  tenant1 := newTenant("tenant-a", ch1)
  tenant2 := newTenant("tenant-b", ch2)

  for _, job := range jobs {
    tenant1.enqueue(job)
    tenant2.enqueue(job)

  } 
  
  close(ch1)
  close(ch2)

  wg.Wait()
}

func receive(wg *sync.WaitGroup, ch chan Job) {
    for v := range ch {
        fmt.Println(v)
    }

    wg.Done()
}
~~~

~~~go
❯ go run .
{job-1 alpha}
fatal error: all goroutines are asleep - deadloc
k!

goroutine 1 [chan send]:
main.Tenant.enqueue(...)
        /Users/sabinonweb/Documents/Projects/que
uebicle/cmd/server/tenant.go:18
main.main()
        /Users/sabinonweb/Documents/Projects/que
uebicle/cmd/server/main.go:39 +0x23c

goroutine 19 [chan receive]:
main.receive(0x2eea3e596020, 0x2eea3e594070)
        /Users/sabinonweb/Documents/Projects/que
uebicle/cmd/server/main.go:50 +0x98
created by main.main in goroutine 1
        /Users/sabinonweb/Documents/Projects/que
uebicle/cmd/server/main.go:17 +0x74

goroutine 20 [chan receive]:
main.receive(0x2eea3e596020, 0x2eea3e594070)
        /Users/sabinonweb/Documents/Projects/que
uebicle/cmd/server/main.go:50 +0x98
created by main.main in goroutine 1
        /Users/sabinonweb/Documents/Projects/que
uebicle/cmd/server/main.go:17 +0x74

goroutine 21 [chan receive]:
main.receive(0x2eea3e596020, 0x2eea3e594070)
        /Users/sabinonweb/Documents/Projects/que
uebicle/cmd/server/main.go:50 +0x98
created by main.main in goroutine 1
        /Users/sabinonweb/Documents/Projects/que
uebicle/cmd/server/main.go:17 +0x74

goroutine 22 [chan receive]:
main.receive(0x2eea3e596020, 0x2eea3e594070)
        /Users/sabinonweb/Documents/Projects/que
uebicle/cmd/server/main.go:50 +0x98
created by main.main in goroutine 1
        /Users/sabinonweb/Documents/Projects/que
uebicle/cmd/server/main.go:17 +0x74

goroutine 23 [chan receive]:
main.receive(0x2eea3e596020, 0x2eea3e594070)
        /Users/sabinonweb/Documents/Projects/que
uebicle/cmd/server/main.go:50 +0x98
created by main.main in goroutine 1
        /Users/sabinonweb/Documents/Projects/que
uebicle/cmd/server/main.go:17 +0x74
exit status 2
(base) 
~~~

The workers are looking at the `channel1` only and there is no receiver for the `channel2` that is what is causing the error.

The obvious fix over here is adding one more for loop for the second channel. But if we do that, that is going to create separate workers for every channel which we don't want.

So let's use `select` which is provided by go itself. It selects from whichever channel is ready. If both are ready at the same time, it selects randomly.

So, adding it in the receive function:

~~~go
func receive(wg *sync.WaitGroup, ch1 chan Job, ch2 chan Job) {
    for {
      select {
        case v := <- ch1: 
          fmt.Println(v)

        case v := <- ch2:
        fmt.Println(v)
      }
    }

    wg.Done()
}
~~~

Change the function signature to receive two channels.

It gives:
~~~
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
{ }
~~~

So, first of all:
 ~~~go
 for v := range ch {
    fmt.Println(v)
}
 ~~~

We had a loop like this. But now we have 

~~~go
for {
  select {
	case v := <- ch1: 
	  fmt.Println(v)

	case v := <- ch2:
	fmt.Println(v)
  }
}
~~~

`<-` is added because `select` can both send and receive from a channel, so specification is needed while `range` only receives. 

### Back to the error
Why did it print infinite `{}`?  Receiving from a channel usually gives `v, ok := <- ch1` . When the value is received `ok = true` and when the value is empty `ok = false`. Once `ok = false` receiving never stops, it gives out empty instances of `Job{}`. `range` handles this internally  by stopping when it sees `ok = false` but `select` doesn't provide us that. 

### Fix /Error

~~~go
func receive(wg *sync.WaitGroup, ch1 chan Job, ch2 chan Job) {
    for {
      select {
        case v, ok := <- ch1:
          if ok {
            fmt.Println(v)
          } else {
            wg.Done()
          }
          

        case v, ok := <- ch2:
        if ok {
            fmt.Println(v)
          } else {
            wg.Done()
          }

      }
    }
}
~~~

```
> go run .
{job-1 alpha}
{job-3 gamma}
{job-4 delta}
{job-4 delta}
{job-2 beta}
{job-2 beta}
{job-1 alpha}
{job-5 epsilon}
{job-6 zeta}
{job-5 epsilon}
{job-3 gamma}
{job-7 eta}
{job-8 theta}
{job-9 iota}
{job-10 kappa}
{job-7 eta}
{job-6 zeta}
{job-8 theta}
{job-9 iota}
{job-10 kappa}
panic: sync: negative WaitGroup counter
```

It shows negative counter because 20 `wg.Done()` gets called for 5 `Add` counters. 

### Fix
~~~go
func receive(wg *sync.WaitGroup, ch1 chan Job, ch2 chan Job) {
    outer: 
    for {
      select {
        case v, ok := <- ch1:
          if ok {
            fmt.Println(v)
          } else {
            break outer
          }

        case v, ok := <- ch2:
        if ok {
            fmt.Println(v)
        } else {
          break outer
        }
        }
    }
    wg.Done()
}
~~~

It works correctly. For a worker when it finishes, for the channel, it breaks and calls `Done`. 

### Issue 
It breaks when only one of the channels hits `!ok` . What if one of them still has jobs to run?

Let's see this in action, rather than just believing it. 

~~~go
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
~~~

It gives:

![[Screenshot 2026-09-12 at 16.55.23.png]]

What happens here is `ch1` runs and causes `select` to exit early. By the time `job-14` is reached all the workers have already exited. 

### Possible Fix

Use two variables that tracks the `ok` status and `break` when the condition is met.

~~~go
for {
	ch1Ok := false
	ch2Ok := false
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

		if ch1Ok && ch2Ok {
				break outer
		}
	}
}
~~~

There is an issue with this one, too. `ch1Ok` and `ch2Ok` are always set to `false` at the start of the `for` loop causing the loop to fall into infinite pattern. Every `worker` is called but it is never exited because condition is never met.

To fix it:

~~~go
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

		if ch1Ok && ch2Ok {
				break outer
		}
	}
}
~~~

`ch1` and `ch2` are set to false then loop is called for a single worker out of 5, it receives its work from either of the channel and if both channels have been observed false it exits.

### Issue

But there is one more hidden issue here. You notice:

~~~go
if ch1Ok && ch2Ok {
	break outer
}
~~~

It is inside the `case2` block. If there is a case when both `ch1Ok` and `ch2Ok` are already true but the select lands on `case1` instead of `case2`. This can happen when both `ok = false` and because of the fact that closed channel always stays ready. It might cause few extra condition checking before finally exiting. 

### Fix

Move the block outside the `select` case. It runs once per iteration.

~~~go
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
~~~

### Dispatcher
`select` lets an `worker/goroutine` wait on multiple channels. It can work well on `compiled` code, but normally tenant jobs appear during `runtime`. 
