#!/bin/sh

SUBJECT="tickers_avro_topic-value"
while [ $(curl -s -o /dev/null -w %{http_code} http://schema-registry:8081/subjects) -ne 200 ]; do
  sleep 5
done

curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data @/schemas/ticker.json \
     http://schema-registry:8081/subjects/$SUBJECT/versions