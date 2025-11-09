#!/bin/bash

echo "== Reset config =="
queuectl config set max_retries 1
queuectl config set backoff_base 1

echo "== Enqueue jobs =="
queuectl enqueue '{"id":"ok1","command":"echo OK"}'
queuectl enqueue '{"id":"bad1","command":"false"}'

echo "== Start worker =="
queuectl worker start --count 1
sleep 4

echo "== Status =="
queuectl status

echo "== Jobs =="
queuectl list

echo "== DLQ =="
queuectl dlq list

echo "== Retry DLQ =="
queuectl dlq retry bad1
queuectl list
