#!/bin/bash

if [ "$#" -ne 4 ]; then
    echo "Provide args: $0 [index] [username] [password] [path]"
    exit 1
fi

index=$1
username=$2
password=$3
path=$4

exportIndex() {
	index=$1
	username=$2
	password=$3
	path=$4
	timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
	filename="$1_$timestamp.json.gz"

	echo "dumping $index"
	sudo elasticdump --input="http://localhost:9200/$index" --output=$ | gzip > "$filename"
	echo "uploading $filename"
	curl -X PUT -u "$username:$password" --progress-bar -T "$filename" "https://owncloud.hpi.de/remote.php/dav/files/$username/$path/$filename" | cat
}


if [[ $index == "all" ]];then
	cmd=$(curl --silent 'http://127.0.0.1:9200/_cat/indices' | cut -d\  -f3)
	readarray -t indices <<< "$cmd"
        for i in "${indices[@]}"; do
		exportIndex $i $username $password $path
	done
else
	exportIndex $index $username $password $path
fi
