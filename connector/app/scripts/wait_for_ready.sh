echo -e "\n[tpp-script] ====Wait for kafka-connect is up"

until [[ $(curl -s -o /dev/null -w '%{http_code}' "http://$CONNECT_REST_ADVERTISED_HOST_NAME:$CONNECT_REST_PORT/connectors") == 200 ]];
do
  sleep 5
done



echo -e "[tpp-script] ====Kafka-connect is up"