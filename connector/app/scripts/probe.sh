readarray -td, connectors < <(tr -d '[:space:]' <<<"$CONNECTORS,"); declare -p connectors;
readarray -td, disabledTasks < <(tr -d '[:space:]' <<<"$DISABLED_TASKS,"); declare -p disabledTasks;

if [ ${#connectors[@]} -eq 0 ] || [ -z "$CONNECTORS" ]; then
  echo "[tpp-script] connectors are empty"
  exit 0
fi

source "$(dirname "$0")"/check_task_status.sh

for c in "${connectors[@]}"; do
  if [[ $c == *":"* ]]; then
    IFS=':' read -r -a templated_module <<< "$c"
    connectorName=${templated_module[0]}
    for task in /tpp/runtime/tpp-"${connectorName}"-connector/tasks/*.json; do
      taskName=$(basename "$task" .json)
      if [[ ${disabledTasks[*]} =~ ${taskName} ]] ; then
        continue
      fi
      checkTaskStatus "$task"
      if [[ "$result" == 0 ]]; then
        exit 0
      fi
    done
  else
    for task in /tpp/runtime/tpp-"${c}"-connector/tasks/*.json; do
      taskName=$(basename "$task" .json)
      if [[ ${disabledTasks[*]} =~ ${taskName} ]] ; then
        continue
      fi
      checkTaskStatus "$task"
      if [[ "$result" == 0 ]]; then
        exit 0
      fi
    done
  fi
done
exit 1