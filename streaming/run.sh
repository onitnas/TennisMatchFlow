until curl -location http://kafka-connect:8083/; do
  echo "Waiting for Kafka Connect..."
  sleep 2
done

echo "Create truststore and keystore..."
openssl pkcs12 -export -out bundle.p12 \
  -in /usr/share/elasticsearch/config/certs/elasticsearch/elasticsearch.crt \
  -inkey /usr/share/elasticsearch/config/certs/elasticsearch/elasticsearch.key \
  -name elasticsearch \
  -passout pass:rootlibero

keytool -import \
  -keystore /usr/share/elasticsearch/config/certs/truststore.jks \
  -file /usr/share/elasticsearch/config/certs/ca/ca.crt \
  -alias cacert \
  -storepass rootlibero \
  -noprompt

keytool -destkeystore /usr/share/elasticsearch/config/certs/keystore.jks \
  -importkeystore \
  -srckeystore bundle.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass rootlibero \
  -deststorepass rootlibero \
  -storepass rootlibero \
  -noprompt

echo "Ensure index template exists..."

curl -sS -u elastic:rootlibero --cacert /usr/share/elasticsearch/config/certs/ca/ca.crt \
  -X PUT 'https://elasticsearch:9200/_index_template/tmf_new_point_template' \
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