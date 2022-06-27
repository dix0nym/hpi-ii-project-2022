#!/bin/bash

protoc --proto_path=proto --python_out=build/gen proto/bakdata/corporate/v1/corporate.proto
protoc --proto_path=proto --python_out=build/gen proto/parsed_hrb/v1/*.proto
protoc --proto_path=proto --python_out=build/gen proto/gleif/v1/*.proto
find build/gen -type f -name '*.py' -exec sed -i 's/from gleif.v1/from build.gen.gleif.v1/g' {} +
