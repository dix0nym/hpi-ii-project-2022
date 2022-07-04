#!/bin/bash

# delete index
curl -X DELETE http://localhost:9200/gf-lei
# create index
curl -X PUT http://localhost:9200/gf-lei
# disable date detection
curl -X PUT http://localhost:9200/gf-lei/_mapping -H 'Content-Type: application/json'  -d '{"date_detection": false}'
# delete index
curl -X DELETE http://localhost:9200/gf-relationship
# create index
curl -X PUT http://localhost:9200/gf-relationship
# disable date detection
curl -X PUT http://localhost:9200/gf-relationship/_mapping -H 'Content-Type: application/json'  -d '{"date_detection": false}'
