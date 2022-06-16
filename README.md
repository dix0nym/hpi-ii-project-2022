# HPI information integration project SoSe 2022

This repository provides a code base for the information integration course in the summer semester of 2022. Below you
can find the documentation for setting up the project.

## Prerequisites

- Install [Poetry](https://python-poetry.org/docs/#installation)
- Install [Docker](https://docs.docker.com/get-docker/) and [docker-compose](https://docs.docker.com/compose/install/)
- Install [Protobuf compiler (protoc)](https://grpc.io/docs/protoc-installation/). If you are using windows you can
  use [this guide](https://www.geeksforgeeks.org/how-to-install-protocol-buffers-on-windows/)
- Install [jq](https://stedolan.github.io/jq/download/)

## Architecture

![](architecture.png)

### GLEIF Crawler

GLEIF is the Gloabl Legal Entity Identifier Foundation and they publish two datasets - level 1 ("who is who") and level 2 ("Who owns whom").
The Legal Entity Identifier (LEI) is a alphanumeric 20 digit code and is used for unique identification of legal entities involved in financial transactions.

Both datasets are available via download from the website, they are compressed with zip. To use the crawler you have to download and extract it the dataset you want to parse.

**Downlod the dataset** form [here](https://www.gleif.org/de/lei-data/gleif-concatenated-file/download-the-concatenated-file)

The Crawler takes the plain xml file as an input, parse them by objects and produces messages to Kafka. For an usage example see [GLEIF Crawler](#gleif-crawler-usage).

We use [Protocol buffers](https://developers.google.com/protocol-buffers) to define our schemas: [gleif](./proto/gleif/v1/gleif.proto), [relationship](./proto/gleif/v1/relationship.proto).

The crawler uses the generated model class (i.e., `LEIRecord` class) from the [protobuf schema](./proto/gleif/v1/gleif.proto).
We will explain furthur how you can generate this class using the protobuf compiler.
The compiler creates a `LEIRecord` class with the fields defined in the schema. The crawler fills the object fields with
the extracted data from the website.

It then serializes the `LEIRecord` object to bytes so that Kafka can read it and produces it to the `gleif`
topic. This process continues until the end of the xml file is reached, and the crawler will stop automatically.
The same procedure for the parsing of the relationships.

### gleif topic

The topic `gleif` holds all the messages produced by the `gleif_crawler`. Each message in a Kafka topic
consist of a key and value.

The key type of this topic is `String`. The key is the unique LEI-ID taken from the dataset. For example: `529900D6BF99LW9R2E68` for the company `SAP SE`.

The value of the message contains more information like `LegalName`, `LegalAddress`, and more. Therefore, the value type
is complex and needs a schema definition - see the [gleif proto schema](./proto/gleif/v1/gleif.proto).

### gleif_relationships topic

The topic `gleif_relationships` holds all the messages produced by the `gleif_crawler`. Each message in a Kafka topic
consist of a key and value.

The key type of this topic is `String`. The key is generated by the `gleif_crawler` and is a composite key consiting of `StartNode.NodeID`, `EndNode.NodeID`, `RegistrationStatus`, `RelationshipStatus` and `InitialRegistrationDate`.

For example a key for the relationship between hybris AG (`529900P3PNL7BZ051X87`) and SAP SE (`529900D6BF99LW9R2E68`) looks like:

`529900P3PNL7BZ051X87_529900D6BF99LW9R2E68_4_1_1498212258`

The value of the message contains more information like `RelationshipPeriod`, `RelationshipQualifier`, and more. Therefore, the value type
is complex and needs a schema definition - see the [relationship proto schema](./proto/gleif/v1/relationship.proto).

### RB Website

The [Registerbekanntmachung website](https://www.handelsregisterbekanntmachungen.de/index.php?aktion=suche) contains
announcements concerning entries made into the companies, cooperatives, and
partnerships registers within the electronic information and communication system. You can search for the announcements.
Each announcement can be requested through the link below. You only need to pass the query parameters `rb_id`
and `land_abk`. For instance, we chose the state Rheinland-Pfalz `rp` with an announcement id of `56267`, the
new entry of the company BioNTech.

```shell
export STATE="rp" 
export RB_ID="56267"
curl -X GET  "https://www.handelsregisterbekanntmachungen.de/skripte/hrb.php?rb_id=$RB_ID&land_abk=$STATE"
```

### RB Crawler

The Registerbekanntmachung crawler (rb_crawler) sends a get request to the link above with parameters (`rb_id`
and `land_abk`) passed to it and extracts the information from the response.

We use [Protocol buffers](https://developers.google.com/protocol-buffers)
to define our [schema](./proto/bakdata/corporate/v1/corporate.proto).

The crawler uses the generated model class (i.e., `Corporate` class) from
the [protobuf schema](./proto/bakdata/corporate/v1/corporate.proto).
We will explain furthur how you can generate this class using the protobuf compiler.
The compiler creates a `Corporate` class with the fields defined in the schema. The crawler fills the object fields with
the
extracted data from the website.
It then serializes the `Corporate` object to bytes so that Kafka can read it and produces it to the `corporate-events`
topic. After that, it increments the `rb_id` value and sends another GET request.
This process continues until the end of the announcements is reached, and the crawler will stop automatically.

**Modifictation**

We modified the [rb_extractor](./rb_crawler/rb_extractor.py) to handle all kinds of events by simply adding the new handler `handle_unkown`. The handler produces a message to Kafka containing the `event_type` and the raw text blob.

The previous check for the return of `Falsche Paremter` as indicator of the end of events is not correct. Because in all cases we have observed, there are following `IDs` after this invalid `ID`.

We determined that there is no really indicator for the end of events. Therefore we implemented an algorithm that takes the current date and a threshold of previous invalid `IDs` (`fail_count`).

We stop crawling in the following two conditions:
- The date in the event is **today** and the `fail_count` is greater than 1000
- The date in the event is **not today** an the `fail_count` is greater than 10000

### corporate-events topic

The `corporate-events` holds all the events (announcements) produced by the `rb_crawler`. Each message in a Kafka topic
consist of a key and value.

The key type of this topic is `String`. The key is generated by the `rb_crawler`. The key
is a combination of the `land_abk` and the `rb_id`. If we consider the `rb_id` and `land_abk` from the example above,
the
key will look like this: `rp_56267`.

The value of the message contains more information like `event_name`, `event_date`, and more. Therefore, the value type
is complex and needs a schema definition.

### RB Parser 

The Parser (rb_parser) ingests the messages in the corporate events topic and extracts information from the text. 

The parser uses a generated model class 'Company' from the [protobuf schema](./proto/parsed_hrb/v1/hrb.proto) for the resulting information.
The parser subscribes to the messages in the corporate-events topic and deserializes them. Then the message is divided into parts and interesting information is extracted. This is a bit difficult because there are quite a lot of typos/missing spaces or accidental puncuation in the HRB data. 
It then serializes the new Company object with the extracted data and produces it to the corporate-events-parsed topic. 

### corporate-events-parsed topic
The 'corporate-events-parsed' topic holds the extracted information from the HRB data. The key is the same as in the corporate-events topic. 
It includes information about the address of a company and the name of the company. It also contains the date of the founding document, the company objective, the initial capital and information about executives. Executives in this case may also be other companies. 

### Kafka Connect

[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) is a tool to move large data sets into
(source) and out (sink) of Kafka.
Here we only use the Sink connector, which consumes data from a Kafka topic into a secondary index such as
Elasticsearch.

We use the [Elasticsearch Sink Connector](https://docs.confluent.io/kafka-connect-elasticsearch/current/overview.html)
to move the data from the `coporate-events` topic into the Elasticsearch.

## Setup

This project uses [Poetry](https://python-poetry.org/) as a build tool.
To install all the dependencies, just run `poetry install`.

This project uses Protobuf for serializing and deserializing objects. We provided a
simple [protobuf schema](./proto/bakdata/corporate/v1/corporate.proto).
Furthermore, you need to generate the Python code for the model class from the proto file.
To do so run the [`generate-proto.sh`](./generate-proto.sh) script (**Important**: modify the script to have the correct include diretory - see below).
This script uses the [Protobuf compiler (protoc)](https://grpc.io/docs/protoc-installation/) to generate the model class
under the `build/gen/bakdata/corporate/v1` folder with the name `corporate_pb2.py`.


build protobuf files using following commands:

```shell
protoc --proto_path=proto --python_out=build/gen proto/gleif/v1/gleif.proto
protoc --proto_path=proto --python_out=build/gen proto/gleif/v1/relationship.proto
protoc --proto_path=proto --python_out=build/gen proto/gleif/v1/registration.proto
# or
protoc --proto_path=proto --python_out=build/gen proto/gleif/v1/*.proto
```

On Ubuntu we had to enable the experimental features of Protobuf Version 3.

```shell
protoc --experimental_allow_proto3_optional --proto_path=proto --python_out=build/gen proto/gleif/v1/*.proto
```

## Run

### Infrastructure

Use `docker-compose up -d` to start all the services: [Zookeeper](https://zookeeper.apache.org/)
, [Kafka](https://kafka.apache.org/), [Schema
Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
, [Kafka REST Proxy]((https://github.com/confluentinc/kafka-rest)), [Kowl](https://github.com/redpanda-data/kowl),
[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html),
and [Elasticsearch](https://www.elastic.co/elasticsearch/). Depending on your system, it takes a couple of minutes
before the services are up and running. You can use a tool
like [lazydocker](https://github.com/jesseduffield/lazydocker)
to check the status of the services.

### Kafka Connect

After all the services are up and running, you need to configure Kafka Connect to use the Elasticsearch sink connector.
The config file is a JSON formatted file. We provided a [basic configuration file](./connect/elastic-sink.json).
You can find more information about the configuration properties on
the [official documentation page](https://docs.confluent.io/kafka-connect-elasticsearch/current/overview.html).

To start the connector, you need to push the JSON config file to Kafka. You can either use the UI dashboard in Kowl or
use the [bash script provided](./connect/push-config.sh). It is possible to remove a connector by deleting it
through Kowl's UI dashboard or calling the deletion API in the [bash script provided](./connect/delete-config.sh).

```json
{
  "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
  "topics": "corporate-events",
  "input.data.format": "PROTOBUF",
  "connection.url": "http://elasticsearch:9200",
  "type.name": "corporate-events",
  "key.ignore": "false",
  "schema.ignore": "true",
  "tasks.max": "1",
  "write.method": "UPSERT"
}
```

Similar connector config for the other topics `gleif` and `gleif_relationships`.

### GLEIF Crawler Usage

You can start the crawler with the command below:

```shell
# parse LEI records
poetry run python gleif_crawler/main.py lei 20220503-gleif-concatenated-file-lei2.xml
# parse relationship records
poetry run python gleif_crawler/main.py rr 20220524-gleif-concatenated-file-rr.xml
```

The first parameter is the type of data you want to parse and produce to Kafka:

- `lei` for the Level 1 dataset of GLEIF - the LEIRecords ("who is who")
- `rr` for the Level 2 dataset of GLEIF - the Relationships ("whom belongs to whom")


### RB Crawler Usage

You can start the crawler with the command below:

```shell
poetry run python rb_crawler/main.py --id $RB_ID --state $STATE
```

The `--id` option is an integer, which determines the initial event in the handelsregisterbekanntmachungen to be
crawled.

The `--state` option takes a string (only the ones listed above). This string defines the state where the crawler should
start from.

You can use the `--help` option to see the usage:

```
Usage: main.py [OPTIONS]

Options:
  -i, --id INTEGER                The rb_id to initialize the crawl from
  -s, --state [bw|by|be|br|hb|hh|he|mv|ni|nw|rp|sl|sn|st|sh|th]
                                  The state ISO code
  --help                          Show this message and exit.
```

## Query data

### Kowl

[Kowl](https://github.com/redpanda-data/kowl) is a web application that helps you manage and debug your Kafka workloads
effortlessly. You can create, update, and delete Kafka resources like Topics and Kafka Connect configs.
You can see Kowl's dashboard in your browser under http://localhost:8080.

### Elasticsearch

To query the data from Elasticsearch, you can use
the [query DSL](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/query-dsl.html) of elastic. For example:

```shell
curl -X GET "localhost:9200/_search?pretty" -H 'Content-Type: application/json' -d'
{
    "query": {
        "match": {
            <field>
        }
    }
}
'
```

`<field>` is the field you wish to search. For example:

```
"reference_id":"HRB 41865"
```

## Teardown
You can stop and remove all the resources by running:
```shell
docker-compose down
```
