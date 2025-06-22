!/bin/bash

set -ex

curl -d '{"name":"queue1"}' -H 'Content-Type: application/json' localhost:8086/create_queue
curl -d '{"name":"queue2"}' -H 'Content-Type: application/json' localhost:8086/create_queue
curl -d '{"name":"queue3"}' -H 'Content-Type: application/json' localhost:8086/create_queue
curl -d '{"name":"queue4"}' -H 'Content-Type: application/json' localhost:8086/create_queue

curl -H 'Content-Type: application/json' localhost:8086/queues

export JOB_1=$(curl -d '{"queue_name":"queue1","payload":"{\"id\":1}"}' -H 'Content-Type: application/json' localhost:8086/job)
export JOB_2=$(curl -d '{"queue_name":"queue1","payload":"{\"id\":2}"}' -H 'Content-Type: application/json' localhost:8086/job)
curl -H 'Content-Type: application/json' 'localhost:8086/jobs?queue=queue1'

curl -H 'Content-Type: application/json' 'localhost:8086/job?queue=queue1'
curl -H 'Content-Type: application/json' 'localhost:8086/jobs?queue=queue1'

sleep 1
curl -d "{\"uuid\": $(echo $JOB_2| jq '.uuid'), \"queue_name\": \"queue1\", \"status\": \"succeeded\"}" -H 'Content-Type: application/json' 'localhost:8086/complete_job'
curl -H 'Content-Type: application/json' 'localhost:8086/jobs?queue=queue1'

curl -H 'Content-Type: application/json' 'localhost:8086/job?queue=queue1'
curl -H 'Content-Type: application/json' 'localhost:8086/jobs?queue=queue1'

sleep 1
curl -d "{\"uuid\": $(echo $JOB_1| jq '.uuid'), \"queue_name\": \"queue1\", \"status\": \"succeeded\"}" -H 'Content-Type: application/json' 'localhost:8086/complete_job'
curl -H 'Content-Type: application/json' 'localhost:8086/jobs?queue=queue1'

curl -H 'Content-Type: application/json' 'localhost:8086/complete_job'
curl -H 'Content-Type: application/json' 'localhost:8086/jobs?queue=queue1'
