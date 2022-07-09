#!/usr/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Provide args: $0 [topic]"
    exit 1
fi

setTopicRetention() {
	topic=$1
	value=$2
	kafka-configs.sh --bootstrap-server localhost:29092 --entity-type topics --entity-name "$topic" --alter --add-config retention.ms="$value"
	kafka-configs.sh --bootstrap-server localhost:29092 --entity-type topics --entity-name "$topic" --alter --add-config retention.bytes="$value"
}

if [ "$1" == "all" ]; then
	cmd=$(kafka-topics.sh  --bootstrap-server=localhost:29092 --list)
	readarray -t topics <<< "$cmd"
	for i in "${topics[@]}"; do
		if [[ "$i" =~ ^[^_].* ]]; then
			echo "setting retention to -1 for ${i}"
			setTopicRetention "$i" "-1"
		fi
	done
else
	setTopicRetention $1 "-1"
fi
