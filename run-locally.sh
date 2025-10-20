#!/usr/bin/env bash

# Clean previous local publish of sparklear and publish the current version
find ~/.ivy2 -type f -name *sparklear* | xargs rm
sbt publishLocal 

# Destination for JsonSink outputs (unique per run, under /tmp/sparklear)
mkdir -p "/tmp/sparklear"
DEST=$(mktemp -d /tmp/sparklear/listener.XXXXXX)
echo "JsonSink destination: $DEST"

spark-shell \
  --packages com.amadeus:sparklear_spark352_2.12:0.0.29-SNAPSHOT \
  --conf spark.extraListeners=com.amadeus.sparklear.SparklEarListener \
  --conf spark.sparklear.sink.class=com.amadeus.sparklear.JsonSink \
  --conf spark.sparklear.sink.json.destination="$DEST"
