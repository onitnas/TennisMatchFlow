/etc/confluent/docker/run &

until curl -location http://localhost:8083/; do
  echo "Waiting for Kafka Connect..."
  sleep 2
done

curl --location 'http://localhost:8083/connectors' \
  -H "Content-Type: application/json" \
  -d @/connectors/elastic-sink.json

wait