#!/bin/sh
set -e
export PATH="/opt/kafka/bin:$PATH"
BOOTSTRAP="kafka:9093"

echo "Creating topics..."
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic demo-events --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic demo-metrics --partitions 1 --replication-factor 1

echo "Producing sample events to demo-events..."
echo '{"user_id":1,"event":"login","ip":"192.168.1.1"}' | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic demo-events
echo '{"user_id":1,"event":"page_view","path":"/dashboard"}' | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic demo-events
echo '{"user_id":2,"event":"login","ip":"10.0.0.5"}' | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic demo-events
echo '{"user_id":2,"event":"purchase","amount":99.99}' | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic demo-events
echo '{"user_id":3,"event":"signup"}' | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic demo-events

echo "Producing sample metrics to demo-metrics..."
echo '{"ts":'$(date +%s000)',"name":"cpu_usage","value":12.5,"tags":"host=server1"}' | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic demo-metrics
echo '{"ts":'$(date +%s000)',"name":"memory_mb","value":2048,"tags":"host=server1"}' | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic demo-metrics
echo '{"ts":'$(date +%s000)',"name":"cpu_usage","value":8.1,"tags":"host=server2"}' | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic demo-metrics

echo "Kafka init done."
