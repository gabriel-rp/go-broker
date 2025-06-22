package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func testQueue() (string, *Queue) {
	queueName := "testQueue"
	err := CreateQueue(queueName)
	if err != nil {
		panic("Couldn't create test Queue")
	}
	queue, err := GetQueue(queueName)
	if err != nil || queue == nil {
		panic("Couldn't create test Queue")
	}
	return queueName, queue
}

func TestCreateQueue(t *testing.T) {
	err := CreateQueue("myQueue")
	assert.NoError(t, err)

	queue, err := GetQueue("myQueue")
	assert.Equal(t, "myQueue", queue.Name)
	assert.True(t, queue.PendingJobs.IsEmpty())
	assert.NoError(t, err)
}

func TestRecreateQueue(t *testing.T) {
	err := CreateQueue("myQueue")
	assert.NoError(t, err)

	err = CreateQueue("myQueue")
	assert.Error(t, err)
}

func TestGetNonExistentQueue(t *testing.T) {
	queue, err := GetQueue("nonExistentQueue")
	assert.Nil(t, queue)
	assert.Error(t, err)
}

func TestGetExistingQueue(t *testing.T) {
	err := CreateQueue("newQueue")
	assert.NoError(t, err)
	queue, err := GetQueue("newQueue")
	assert.NoError(t, err)
	assert.NotNil(t, queue)
}

func TestListQueues(t *testing.T) {
	err := CreateQueue("newQueue1")
	assert.NoError(t, err)
	err = CreateQueue("newQueue2")
	assert.NoError(t, err)
	err = CreateQueue("newQueue3")
	assert.NoError(t, err)
	queueNames := ListQueues()
	assert.Equal(t, queueNames, []string{"newQueue1", "newQueue2", "newQueue3"})
}

func TestAddPendingJob(t *testing.T) {
	queueName, queue := testQueue()
	queueJob := NewQueueJob(Job{Payload: "{}"})
	AddPendingJob(queueName, queueJob)
	lastItem, ok := queue.PendingJobs.Peek()
	assert.True(t, ok)
	assert.Equal(t, lastItem, queueJob)
}

func TestQueueSize(t *testing.T) {
	queueName, queue := testQueue()
	assert.Equal(t, 0, queue.PendingJobs.Size())

	err := AddPendingJob(queueName, NewQueueJob(Job{Payload: "{}"}))
	assert.NoError(t, err)
	assert.Equal(t, 1, queue.PendingJobs.Size())

	AddPendingJob(queueName, NewQueueJob(Job{Payload: "{}"}))
	assert.Equal(t, 2, queue.PendingJobs.Size())
}

func TestDequeueJob(t *testing.T) {
	queueName, queue := testQueue()

	// Setup
	err := AddPendingJob(queueName, NewQueueJob(Job{Payload: "{}"}))
	assert.NoError(t, err)
	err = AddPendingJob(queueName, NewQueueJob(Job{Payload: "{}"}))
	assert.NoError(t, err)
	assert.Equal(t, 2, queue.PendingJobs.Size())
	assert.Equal(t, 0, len(queue.RunningJobs))
	assert.Equal(t, 0, len(queue.CompletedJobs))

	// Dequeue
	queueJob, ok := queue.DequeueJob()
	assert.True(t, ok)
	assert.Equal(t, 1, queue.PendingJobs.Size())
	assert.Equal(t, 1, len(queue.RunningJobs))
	assert.Equal(t, 0, len(queue.CompletedJobs))
	// Successful processing
	queue.CompleteJob(queueJob.UUID, JobStatusSucceeded)
	assert.Equal(t, 1, queue.PendingJobs.Size())
	assert.Equal(t, 0, len(queue.RunningJobs))
	assert.Equal(t, 1, len(queue.CompletedJobs))

	// Dequeue
	queueJob, ok = queue.DequeueJob()
	assert.True(t, ok)
	assert.Equal(t, 0, queue.PendingJobs.Size())
	assert.Equal(t, 1, len(queue.RunningJobs))
	assert.Equal(t, 1, len(queue.CompletedJobs))
	// Unsuccessful processing
	queue.CompleteJob(queueJob.UUID, JobStatusFailed)
	assert.Equal(t, 1, queue.PendingJobs.Size())
	assert.Equal(t, 0, len(queue.RunningJobs))
	assert.Equal(t, 1, len(queue.CompletedJobs))
}

func TestDequeueJobEmptyQueue(t *testing.T) {
	_, queue := testQueue()
	_, ok := queue.DequeueJob()
	assert.False(t, ok)
}
