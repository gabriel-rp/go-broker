package broker

import (
	"errors"
	"fmt"
	"time"

	"github.com/gabriel-rp/go-broker/pkg"
	"github.com/google/uuid"
)

const (
	JobStatusSucceeded JobStatus = iota
	JobStatusFailed
)

type JobStatus int
type Job struct {
	Payload string `json:"payload"`
}
type QueueJob struct {
	UUID           uuid.UUID `json:"uuid,omitempty"`
	Job            Job       `json:"job,omitempty"`
	Attempts       int       `json:"attempts"`
	TimeoutSeconds int       `json:"timeout_seconds,omitempty"`
	CreatedAt      int64     `json:"created_at,omitempty"`
	MaxAttempts    int       `json:"max_attempts,omitempty"`
}
type Queue struct {
	Name          string
	PendingJobs   pkg.Stack[QueueJob]
	CompletedJobs []QueueJob
	RunningJobs   map[uuid.UUID]QueueJob
}

type Broker struct {
	Queues map[string]*Queue
}

func NewBroker() Broker {
	return Broker{
		Queues: make(map[string]*Queue),
	}
}

func NewQueueJob(job Job) QueueJob {
	return QueueJob{
		Job:            job,
		UUID:           uuid.New(),
		Attempts:       0,
		TimeoutSeconds: 120,
		MaxAttempts:    3,
		CreatedAt:      time.Now().Unix(),
	}
}

func NewQueue(queueName string) Queue {
	return Queue{
		Name:          queueName,
		PendingJobs:   pkg.NewStack[QueueJob](),
		CompletedJobs: make([]QueueJob, 0),
		RunningJobs:   make(map[uuid.UUID]QueueJob, 0),
	}
}

func (q *Queue) DequeueJob() (QueueJob, bool) {
	queueJob, ok := q.PendingJobs.Pop()
	if !ok {
		return queueJob, ok
	}
	q.RunningJobs[queueJob.UUID] = queueJob
	return queueJob, ok
}

func (q *Queue) CompleteJob(uuid uuid.UUID, status JobStatus) error {
	queueJob, ok := q.RunningJobs[uuid]
	if !ok {
		return fmt.Errorf("Job not found: '%v'", uuid)
	}
	if status != JobStatusSucceeded {
		delete(q.RunningJobs, uuid)
		q.PendingJobs.Push(queueJob)
		return nil
	}

	q.CompletedJobs = append(q.CompletedJobs, queueJob)
	delete(q.RunningJobs, uuid)
	return nil
}

func (b *Broker) CreateQueue(queueName string) error {
	if _, ok := b.Queues[queueName]; ok {
		return errors.New("Queue already exists")
	}
	newQueue := NewQueue(queueName)
	b.Queues[queueName] = &newQueue
	return nil
}

func (b *Broker) GetQueue(queueName string) (*Queue, error) {
	queue, ok := b.Queues[queueName]
	if ok {
		return queue, nil
	} else {
		return nil, fmt.Errorf("Queue not found: '%v'", queueName)
	}
}

func (b *Broker) ListQueues() []string {
	queueNames := make([]string, 0)
	for queueName := range b.Queues {
		queueNames = append(queueNames, queueName)
	}
	return queueNames
}

func (b *Broker) AddPendingJob(queueName string, job QueueJob) error {
	queue, err := b.GetQueue(queueName)
	if err != nil {
		return err
	}
	queue.PendingJobs.Push(job)
	return nil
}
