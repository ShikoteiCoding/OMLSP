# OMLSP
OMLSP is a single-node streaming processor written in Python, inspired by RisingWave and Apache Flink.

It enables you to define streaming data pipelines using SQL transformations, following a Flink-like syntax (extended ANSI-sql).

OMLSP is primarily designed to handle HTTP-based data sources and perform HTTP lookups, making it ideal for integrating with APIs, webhooks, and microservices.

## What OMLSP has

OMLSP orchestrates data flow pipelines from HTTP sources to output sinks with:

- Strong typing – Schema-first design for reliable pipelines.
- SQL-first transformations – Use familiar SQL to define dataflows and streaming logic.
- Dynamic lookups – Combine live HTTP endpoints with batch data for enriched transformations.
- DuckDB-powered execution – Leverages DuckDB
 for ultra-fast, in-process analytics.

The engine continuously fetches, transforms, and routes data asynchronously using Trio
, with channels acting as internal communication streams (similar to Go channels).

## What OMSL doesn't have

- Streaming joins. i.e multiple upstream (join with 1 lookup is possible)
- Failover recovery
- Stateful tasks

## Quickstart

OMLSP requires python > 3.13 (just because) and uv.

In one terminal:
```sh
make server
```
Once started, in a second terminal
```sh
make client
```

The client is the prefered way of interacting with the streaming processor, if you want to start with an entrypoint, you can also supply a sql filepath to the main function.


```sql
-- Create a table which keeps all tickers list in batch
CREATE TABLE all_tickers (
    symbol STRING,
    symbolName STRING,
    buy FLOAT,
    sell FLOAT
)
WITH (
    'connector' = 'http',
    'url' = 'https://api.kucoin.com/api/v1/market/allTickers',
    'method' = 'GET',
    'schedule' = '*/1 * * * *',
    'jq' = '.data.ticker[:2][] | {symbol, symbolName, buy, sell}',
    'headers.Content-Type' = 'application/json'
);

-- Create an output destination to kafka
CREATE SINK all_tickers_sink_bis FROM (SELECT symbolName, buy FROM all_tickers)
WITH (
    connector = 'kafka',
    topic = 'tickers_topic_2',
    server = 'localhost:9092',
);
```

Inspect your entities:
```sql
SHOW TABLES;
```

Drop your entities:
```sql
DROP TABLE CASCADE all_tickers;
```

Check examples in `examples` folder, such as:
- Websockets
- Generated columns
- Secrets
- Kafka sink
- Views / Materialized views
- Lookup joins

### Supported syntaxes

```
CREATE {TABLE, SOURCE, SINK, SECRET, VIEW, MATERIALIZED VIEW}
DROP {TABLE, SOURCE, SINK, SECRET, VIEW, MATERIALIZED VIEW} {CASCADE}
SHOW {TABLES, SOURCES, SINKS, SECRETS, VIEWS}
```

## Architecture
This project implements a lightweight asynchronous SQL orchestration engine using DuckDB and Trio.

The central component is the Runner class, which coordinates client SQL submissions, query evaluation, and task scheduling, using asynchronous channels (similar to Go channels).

Everything happens asynchronously, with each component communicating through Channel objects.

Core components:
- App: Main actor, used to dispatch sql to relevant managers
- ClientManager: Interface with clients for SQL (act as a terminal)
- EntityManager: Translate SQL commands (Create / Drop) into backend entities. Forward task commands to TaskManager for execution.
- TaskManager: Schedule task commands and handle their lifecycle
- ChannelBroker: Centralized Channel broker registry for anonymous dynamic publishes

# Usage

## Dependencies
- Python >= 3.13.1
- uv package manager

## Installation
As we do not have pip for this small project, you will have to create a clean environment and build the package yourself.

In your virtual environment
```shell
uv sync --active
make server
```

Additionally, in another  console / virtual environmment, you can provide SQL statements through the interactive shell client
```shell
make client
```