#!/usr/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Provide args: $0 [topic]"
    exit 1
fi

topic=$1
kafka-configs.sh --bootstrap-server localhost:29092 --entity-type topics --entity-name "$topic" --alter --add-config retention.ms=-1
kafka-configs.sh --bootstrap-server localhost:29092 --entity-type topics --entity-name "$topic" --alter --add-config retention.bytes=-1
