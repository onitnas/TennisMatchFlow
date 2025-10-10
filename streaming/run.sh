until curl -location http://kafka-connect:8083/; do
  echo "Waiting for Kafka Connect..."
  sleep 2
done

echo "Ensure index template exists..."

curl -sS -X PUT 'http://elasticsearch:9200/_index_template/tmf_new_point_template' \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "index_patterns": ["new_point*"],
    "template": {
      "mappings": {
        "properties": {
          "receiver_score": { "type": "keyword" },
          "server_score": { "type": "keyword" }
        }
      }
    }
  }'

echo "Creating Elasticsearch sink connector..."

curl --location 'http://kafka-connect:8083/connectors' \
  -H "Content-Type: application/json" \
  -d @/connectors/elastic-sink.json

wait