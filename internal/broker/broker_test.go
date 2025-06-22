package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func testBroker() *Broker {
	broker := NewBroker()
	return &broker
}

func testQueue(broker *Broker) (string, *Queue) {
	queueName := "testQueue"
	err := broker.CreateQueue(queueName)
	if err != nil {
		panic("Couldn't create test Queue")
	}
	queue, err := broker.GetQueue(queueName)
	if err != nil || queue == nil {
		panic("Couldn't create test Queue")
	}
	return queueName, queue
}

func TestCreateQueue(t *testing.T) {
	broker := testBroker()
	err := broker.CreateQueue("myQueue")
	assert.NoError(t, err)

	queue, err := broker.GetQueue("myQueue")
	assert.Equal(t, "myQueue", queue.Name)
	assert.True(t, queue.PendingJobs.IsEmpty())
	assert.NoError(t, err)
}

func TestRecreateQueue(t *testing.T) {
	broker := testBroker()
	err := broker.CreateQueue("myQueue")
	assert.NoError(t, err)

	err = broker.CreateQueue("myQueue")
	assert.Error(t, err)
}

func TestGetNonExistentQueue(t *testing.T) {
	broker := testBroker()
	queue, err := broker.GetQueue("nonExistentQueue")
	assert.Nil(t, queue)
	assert.Error(t, err)
}

func TestGetExistingQueue(t *testing.T) {
	broker := testBroker()
	err := broker.CreateQueue("newQueue")
	assert.NoError(t, err)
	queue, err := broker.GetQueue("newQueue")
	assert.NoError(t, err)
	assert.NotNil(t, queue)
}

func TestListQueues(t *testing.T) {
	broker := testBroker()
	err := broker.CreateQueue("newQueue1")
	assert.NoError(t, err)
	err = broker.CreateQueue("newQueue2")
	assert.NoError(t, err)
	err = broker.CreateQueue("newQueue3")
	assert.NoError(t, err)
	queueNames := broker.ListQueues()
	assert.ElementsMatch(t, queueNames, []string{"newQueue1", "newQueue2", "newQueue3"})
}

func TestAddPendingJob(t *testing.T) {
	broker := testBroker()
	queueName, queue := testQueue(broker)
	queueJob := NewQueueJob(Job{Payload: "{}"})
	broker.AddPendingJob(queueName, queueJob)
	lastItem, ok := queue.PendingJobs.Peek()
	assert.True(t, ok)
	assert.Equal(t, lastItem, queueJob)
}

func TestQueueSize(t *testing.T) {
	broker := testBroker()
	queueName, queue := testQueue(broker)
	assert.Equal(t, 0, queue.PendingJobs.Size())

	err := broker.AddPendingJob(queueName, NewQueueJob(Job{Payload: "{}"}))
	assert.NoError(t, err)
	assert.Equal(t, 1, queue.PendingJobs.Size())

	broker.AddPendingJob(queueName, NewQueueJob(Job{Payload: "{}"}))
	assert.Equal(t, 2, queue.PendingJobs.Size())
}

func TestDequeueJob(t *testing.T) {
	broker := testBroker()
	queueName, queue := testQueue(broker)

	// Setup
	err := broker.AddPendingJob(queueName, NewQueueJob(Job{Payload: "{}"}))
	assert.NoError(t, err)
	err = broker.AddPendingJob(queueName, NewQueueJob(Job{Payload: "{}"}))
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
	broker := testBroker()
	_, queue := testQueue(broker)
	_, ok := queue.DequeueJob()
	assert.False(t, ok)
}
