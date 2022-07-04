#!/usr/bin/bash

curl -X DELETE http://localhost:8081/subjects/gleif-value
curl -X DELETE http://localhost:8081/subjects/gleif_relationships-value
curl -X DELETE http://localhost:8081/subjects/gleif%2Fv1%2Frelationship.proto
curl -X DELETE http://localhost:8081/subjects/gleif%2Fv1%2Fregistration.proto
