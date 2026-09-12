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

func (t Tenant) enqueue(job Job) {
  t.queue <- job
}
