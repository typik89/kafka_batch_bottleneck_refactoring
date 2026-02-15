echo -e "\n[script] ====Delete tasks"

connectors=$(curl -s "http://$CONNECT_REST_ADVERTISED_HOST_NAME:$CONNECT_REST_PORT/connectors" | tr -d '"[]')
if [ -z "${connectors}" ]; then
  echo "[script] current connectors are empty"
else
  echo "[script] current connectors=$connectors"
  readarray -td, connectors_arr < <(tr -d '[:space:]' <<<"$connectors,"); declare -p connectors_arr;

  for c in "${connectors_arr[@]}"; do
    rebalanceInProgress=true
    retryLimit=10
    retryCounter=0
    http_code=0
    content=""

    while [[ $rebalanceInProgress == true ]] && (( retryCounter < retryLimit ));
    do
      response=$(curl -X DELETE -sS -w '\n%{http_code}' "http://$CONNECT_REST_ADVERTISED_HOST_NAME:$CONNECT_REST_PORT/connectors/$c")
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

    if (( http_code >= 200 && http_code < 300 )); then
      echo "[script] connector $c deleted"
    else
      echo "[script] connector $c not deleted: "
      echo "[script]  > http_code=$http_code"
      echo "[script]  > content=$content"
      exit 1
    fi
  done
fi



echo -e "[script] ====Tasks deleted"