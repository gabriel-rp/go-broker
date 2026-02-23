package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStartServer(t *testing.T) {
	main()
}

func TestListQueuesHandler(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/queues", nil)
	w := httptest.NewRecorder()

	server := NewServer()
	server.listQueues(w, r)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "[]\n", w.Body.String())
}

func TestListJobsBeforeQueueHandler(t *testing.T) {
	body := strings.NewReader("queue=myQueue")
	r := httptest.NewRequest(http.MethodGet, "/jobs", body)

	w := httptest.NewRecorder()

	server := NewServer()
	server.listJobs(w, r)

	resp := w.Result()
	defer resp.Body.Close()

	// queue does not exist
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGetJobHandler(t *testing.T) {
	body := strings.NewReader("queue=myQueue")
	r := httptest.NewRequest(http.MethodGet, "/job", body)

	w := httptest.NewRecorder()

	server := NewServer()
	server.getJob(w, r)

	resp := w.Result()
	defer resp.Body.Close()

	// queue does not exist
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestAddQueueHandler(t *testing.T) {
	body := strings.NewReader(`{"name": "myQueue"}`)
	r := httptest.NewRequest(http.MethodPost, "/add_queue", body)
	w := httptest.NewRecorder()

	server := NewServer()
	server.addQueue(w, r)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusCreated, resp.StatusCode)
}

func TestAddJob(t *testing.T)
