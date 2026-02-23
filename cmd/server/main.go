package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gabriel-rp/go-broker/internal/broker"
	"github.com/google/uuid"
)

type Server struct {
	broker broker.Broker
	mu     sync.Mutex
}

type JobResponse struct {
	Job        *broker.QueueJob `json:"job,omitempty"`
	EmptyQueue bool             `json:"empty_queue"`
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

func NewServer() *Server {
	return &Server{
		broker: broker.NewBroker(),
		mu:     sync.Mutex{},
	}
}

func (s *Server) listQueues(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	queueNames := s.broker.ListQueues()
	json.NewEncoder(w).Encode(queueNames)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	queueName := r.FormValue("queue")
	queue, err := s.broker.GetQueue(queueName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	queueName := r.URL.Query().Get("queue")
	queue, err := s.broker.GetQueue(queueName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	emptyQueue := false
	job, ok := queue.DequeueJob()
	if !ok {
		job = &broker.QueueJob{}
		emptyQueue = !ok
	}

	json.NewEncoder(w).Encode(JobResponse{
		Job:        job,
		EmptyQueue: emptyQueue,
	})
}

func (s *Server) completeJob(w http.ResponseWriter, r *http.Request) {
	var completeJob CompleteJobPayload
	var queue *broker.Queue
	json.NewDecoder(r.Body).Decode(&completeJob)

	queue, err := s.broker.GetQueue(completeJob.QueueName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	status := broker.JobStatusFailed
	if completeJob.Status == "succeeded" {
		status = broker.JobStatusSucceeded
	}

	err = queue.CompleteJob(completeJob.UUID, status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) addQueue(w http.ResponseWriter, r *http.Request) {
	var createQueue AddQueuePayload
	json.NewDecoder(r.Body).Decode(&createQueue)
	if err := s.broker.CreateQueue(createQueue.Name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *Server) addJob(w http.ResponseWriter, r *http.Request) {
	var createJob AddJobPayload
	json.NewDecoder(r.Body).Decode(&createJob)
	queueJob := broker.NewQueueJob(broker.Job{Payload: createJob.Payload})

	err := s.broker.AddPendingJob(createJob.QueueName, queueJob)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(AddJobResponse{UUID: queueJob.UUID})
}

func main() {
	mux := http.NewServeMux()

	server := NewServer()

	mux.HandleFunc("POST /add_queue", server.addQueue)
	mux.HandleFunc("POST /job", server.addJob)
	mux.HandleFunc("POST /complete_job", server.completeJob)
	mux.HandleFunc("GET /queues", server.listQueues)
	mux.HandleFunc("GET /jobs", server.listJobs)
	mux.HandleFunc("GET /job", server.getJob)

	log.Println("Listening on :8086")
	http.ListenAndServe(":8086", mux)
}
