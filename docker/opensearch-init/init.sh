#!/bin/sh
set -e
URL="${OPENSEARCH_URL:-http://opensearch:9200}"

echo "Waiting for OpenSearch at $URL..."
for i in 1 2 3 4 5 6 7 8 9 10; do
  if curl -sf "$URL" > /dev/null; then
    echo "OpenSearch is ready."
    break
  fi
  if [ "$i" -eq 10 ]; then
    echo "OpenSearch did not become ready in time."
    exit 1
  fi
  sleep 3
done

echo "Creating index panopticum_demo..."
curl -sf -X PUT "$URL/panopticum_demo" -H "Content-Type: application/json" -d '{
  "mappings": {
    "properties": {
      "message": { "type": "text" },
      "level": { "type": "keyword" },
      "created_at": { "type": "date" },
      "user_id": { "type": "integer" }
    }
  }
}' || true

echo "Indexing sample documents..."
curl -sf -X POST "$URL/_bulk" -H "Content-Type: application/x-ndjson" --data-binary '
{"index":{"_index":"panopticum_demo","_id":"1"}}
{"message":"Application started","level":"info","created_at":"2025-02-12T10:00:00Z","user_id":1}
{"index":{"_index":"panopticum_demo","_id":"2"}}
{"message":"User login","level":"info","created_at":"2025-02-12T10:01:00Z","user_id":1}
{"index":{"_index":"panopticum_demo","_id":"3"}}
{"message":"Page view /dashboard","level":"info","created_at":"2025-02-12T10:02:00Z","user_id":1}
{"index":{"_index":"panopticum_demo","_id":"4"}}
{"message":"User login","level":"info","created_at":"2025-02-12T10:03:00Z","user_id":2}
{"index":{"_index":"panopticum_demo","_id":"5"}}
{"message":"Purchase completed","level":"info","created_at":"2025-02-12T10:04:00Z","user_id":2}
{"index":{"_index":"panopticum_demo","_id":"6"}}
{"message":"Error processing request","level":"error","created_at":"2025-02-12T10:05:00Z","user_id":null}
{"index":{"_index":"panopticum_demo","_id":"7"}}
{"message":"Cache cleared","level":"debug","created_at":"2025-02-12T10:06:00Z","user_id":null}
{"index":{"_index":"panopticum_demo","_id":"8"}}
{"message":"Metrics collected","level":"info","created_at":"2025-02-12T10:07:00Z","user_id":null}
'

echo "Refreshing index..."
curl -sf -X POST "$URL/panopticum_demo/_refresh" > /dev/null || true

echo "OpenSearch init done."
