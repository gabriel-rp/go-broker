package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gabriel-rp/go-broker/internal/broker"
	"github.com/google/uuid"
)

var Broker broker.Broker

type JobResponse struct {
	Job        broker.QueueJob `json:"job,omitempty"`
	EmptyQueue bool            `json:"empty_queue"`
}
type JobInfo struct {
	UUID    uuid.UUID `json:"uuid"`
	Payload string    `json:"payload"`
}
type JobsResponse struct {
	Jobs map[string][]JobInfo `json:"jobs"`
}
type AddJobResponse struct {
	UUID uuid.UUID `json:"uuid"`
}

type AddQueuePayload struct {
	Name string `json:"name"`
}
type AddJobPayload struct {
	QueueName string `json:"queue_name"`
	Payload   string `json:"payload"`
}
type CompleteJobPayload struct {
	QueueName string    `json:"queue_name"`
	UUID      uuid.UUID `json:"uuid"`
	Status    string    `json:"status"`
}

func listQueues(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	queueNames := Broker.ListQueues()
	json.NewEncoder(w).Encode(queueNames)
}

func listJobs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	queueName := r.FormValue("queue")
	queue, err := Broker.GetQueue(queueName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jobs := make(map[string][]JobInfo, 3)
	jobs["completed"] = make([]JobInfo, 0, len(queue.CompletedJobs))
	jobs["pending"] = make([]JobInfo, 0, queue.PendingJobs.Size())
	jobs["running"] = make([]JobInfo, 0, len(queue.RunningJobs))

	for _, job := range queue.CompletedJobs {
		jobs["completed"] = append(jobs["completed"], JobInfo{
			UUID:    job.UUID,
			Payload: job.Job.Payload,
		})
	}

	for _, job := range queue.PendingJobs.Items() {
		jobs["pending"] = append(jobs["pending"], JobInfo{
			UUID:    job.UUID,
			Payload: job.Job.Payload,
		})
	}

	for _, job := range queue.RunningJobs {
		jobs["running"] = append(jobs["running"], JobInfo{
			UUID:    job.UUID,
			Payload: job.Job.Payload,
		})
	}

	json.NewEncoder(w).Encode(JobsResponse{
		Jobs: jobs,
	})
}

func getJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	queueName := r.URL.Query().Get("queue")
	queue, err := Broker.GetQueue(queueName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	emptyQueue := false
	job, ok := queue.DequeueJob()
	if !ok {
		job = broker.QueueJob{}
		emptyQueue = !ok
	}

	json.NewEncoder(w).Encode(JobResponse{
		Job:        job,
		EmptyQueue: emptyQueue,
	})
}

func completeJob(w http.ResponseWriter, r *http.Request) {
	var completeJob CompleteJobPayload
	var queue *broker.Queue
	json.NewDecoder(r.Body).Decode(&completeJob)

	queue, err := Broker.GetQueue(completeJob.QueueName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	status := broker.JobStatusFailed
	if completeJob.Status == "succeeded" {
		status = broker.JobStatusFailed
	}

	err = queue.CompleteJob(completeJob.UUID, status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func addQueue(w http.ResponseWriter, r *http.Request) {
	var createQueue AddQueuePayload
	json.NewDecoder(r.Body).Decode(&createQueue)
	if err := Broker.CreateQueue(createQueue.Name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func addJob(w http.ResponseWriter, r *http.Request) {
	var createJob AddJobPayload
	json.NewDecoder(r.Body).Decode(&createJob)
	queueJob := broker.NewQueueJob(broker.Job{Payload: createJob.Payload})

	err := Broker.AddPendingJob(createJob.QueueName, queueJob)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(AddJobResponse{UUID: queueJob.UUID})
}

func main() {
	mux := http.NewServeMux()

	Broker = broker.NewBroker()

	mux.HandleFunc("POST /create_queue", addQueue)
	mux.HandleFunc("POST /job", addJob)
	mux.HandleFunc("POST /complete_job", completeJob)
	mux.HandleFunc("GET /queues", listQueues)
	mux.HandleFunc("GET /jobs", listJobs)
	mux.HandleFunc("GET /job", getJob)

	log.Println("Listening on :8086")
	http.ListenAndServe(":8086", mux)
}
