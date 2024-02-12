# Kafka Tools

A multi-purpose binary for interacting with Kafka for debugging purposes.

## Download and Extract

```sh
curl -L https://github.com/noamtamim/kafka-tools/releases/latest/download/kafka-tools.tgz | tar xz
```

This creates a `kafka-tools` binary in the current directory. You can leave it there or move it elsewhere.

## Usage

Usage: kafka-tools BROKERS ACTION [TOPIC] [OPTIONS]

Actions:
- list [--system]
- consume TOPIC [--raw] [--from-beginning]
- produce TOPIC MESSAGE [--key KEY]
- create TOPIC [--partitions PARTITIONS] [--replication-factor FACTOR] [--min-insync-replicas REPLICAS]
- delete TOPIC [--yes]
