# Kafka Tools

A multi-purpose binary for interacting with Kafka for debugging purposes.

## Download and Extract

```sh
curl -L https://github.com/noamtamim/kafka-tools/releases/latest/download/kafka-tools.tgz | tar xz
```

This creates a `kafka-tools` binary in the current directory. You can leave it there or move it elsewhere.

## Usage

Usage: kafka-tools ACTION BROKERS [TOPIC] [OPTIONS]

list BROKERS [--system]
consume BROKERS TOPIC [--raw] [--from-beginning]
produce BROKERS TOPIC MESSAGE [--key KEY]
create BROKERS TOPIC [--partitions PARTITIONS] [--replication-factor FACTOR]
delete BROKERS TOPIC [--yes]
