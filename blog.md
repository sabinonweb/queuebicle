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

