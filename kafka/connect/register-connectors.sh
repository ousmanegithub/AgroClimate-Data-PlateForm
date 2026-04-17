#!/bin/sh
set -e

CONNECT_URL=${CONNECT_URL:-http://kafka-connect:8083}

echo "[connect-init] waiting for Kafka Connect at ${CONNECT_URL}"
until curl -s "${CONNECT_URL}/connectors" >/dev/null; do
  sleep 2
done

register_connector() {
  connector_name="$1"
  file_path="$2"

  if [ ! -f "$file_path" ]; then
    echo "[connect-init] file not found: $file_path"
    return 1
  fi

  echo "[connect-init] upserting ${connector_name}"
  curl -s -X PUT \
    -H "Content-Type: application/json" \
    --data @"$file_path" \
    "${CONNECT_URL}/connectors/${connector_name}/config" >/dev/null
}

register_connector minio-sink-sensor-raw /connectors/minio-sink-sensor-raw.json
register_connector minio-sink-sensor-anomalies /connectors/minio-sink-sensor-anomalies.json
register_connector minio-sink-field-raw /connectors/minio-sink-field-raw.json
register_connector minio-sink-field-anomalies /connectors/minio-sink-field-anomalies.json

echo "[connect-init] connectors registered"
