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
