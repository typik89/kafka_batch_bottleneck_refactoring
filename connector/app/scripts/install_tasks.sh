echo -e "\n[script] ====Create tasks"


function createTask() {
  rebalanceInProgress=true
  retryLimit=10
  retryCounter=0
  http_code=0
  content=""

  while [[ $rebalanceInProgress == true ]] && (( retryCounter < retryLimit ));
  do
    taskName=$(basename "$task" .json)
    response=$(curl -X PUT -H "Content-Type: application/json" -sS -w '\n%{http_code}' --data @"$task" "http://$CONNECT_REST_ADVERTISED_HOST_NAME:$CONNECT_REST_PORT/connectors/$taskName/config")
    http_code=$(tail -n1 <<< "$response")
    content=$(sed '$ d' <<< "$response")

    if (( http_code == 409 )); then
      rebalanceInProgress=true
      echo "[script] rebalance is in process. wait..."
      ((retryCounter++))
      sleep 5
    else
      rebalanceInProgress=false
    fi
  done

  if (( http_code >= 200 && http_code < 300)); then
    echo "[script] task $taskName created"
  else
    echo "[script] task $taskName not created: "
    echo "[script]  > http_code=$http_code"
    echo "[script]  > content=$content"
  fi
}

for task in /app/tasks/*.json; do
  taskName=$(basename "$task" .json)
  if [[ ${disabledTasks[*]} =~ ${taskName} ]] ; then
    echo "[script] task $taskName disabled"
    continue
  fi
  createTask
done

echo -e "[script] ====Tasks created"