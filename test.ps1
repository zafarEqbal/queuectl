Write-Output "== Reset config =="
queuectl config set max_retries 1 | Out-Null
queuectl config set backoff_base 1 | Out-Null

Write-Output "`n== Enqueue jobs =="
queuectl enqueue "{\"id\":\"ok1\",\"command\":\"echo OK_WIN\"}"
queuectl enqueue "{\"id\":\"bad1\",\"command\":\"nonexistent_command_123\"}"

Write-Output "`n== Start worker =="
queuectl worker start --count 1
Start-Sleep -Seconds 4

Write-Output "`n== Status =="
queuectl status

Write-Output "`n== Jobs =="
queuectl list

Write-Output "`n== DLQ =="
queuectl dlq list

Write-Output "`n== Retry DLQ =="
queuectl dlq retry bad1
queuectl list
