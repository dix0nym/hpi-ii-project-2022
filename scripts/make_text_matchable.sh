#!/bin/bash

curl -X PUT http://localhost:9200/ceo/_mapping -H 'Content-Type: application/json' -d '{"properties": {"birthdate":{"type":"text", "fielddata": true}}}'
curl -X PUT http://localhost:9200/ceo/_mapping -H 'Content-Type: application/json' -d '{"properties": {"firstname":{"type":"text", "fielddata": true}}}'
