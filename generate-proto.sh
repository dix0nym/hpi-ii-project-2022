#!/bin/bash

protoc --proto_path=proto --python_out=build/gen proto/bakdata/corporate/v1/corporate.proto
protoc --proto_path=proto -I include/ --python_out=build/gen proto/gleif/v1/*.proto