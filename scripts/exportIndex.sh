#!/bin/bash

if [ "$#" -ne 5 ]; then
    echo "Provide args: $0 [index] [filename]"
    exit 1
fi

index=$1
filename=$2
username=$3
password=$4
path=$5

echo "dumping $index"
sudo elasticdump --input="http://localhost:9200/$index" --output=$ | gzip > "$filename"

echo "uploading "$filename" to owncloud"
curl -X PUT -u "$username:$password" --progress-bar -T "$filename" "https://owncloud.hpi.de/remote.php/dav/files/$username/$path/$filename" | cat
