# Kafka cheat sheet

Commands for the Kafka broker that sits between Binance and the streaming jobs. The broker uses `SASL_PLAINTEXT` with the `PLAIN` mechanism. Its address and credentials come from your `.env` (see `.env.example` at the repo root) and never live in the code.

## 1. Local Kafka client

```bash
sudo yum install java-17-amazon-corretto -y          # or any Java 17
curl -L -O https://downloads.apache.org/kafka/3.9.2/kafka_2.13-3.9.2.tgz
tar -xzf kafka_2.13-3.9.2.tgz

export KAFKA_HOME="$HOME/kafka_2.13-3.9.2"
set -a; source .env; set +a                          # loads KAFKA_BOOTSTRAP_SERVERS, KAFKA_USERNAME, KAFKA_PASSWORD
export BOOTSTRAP="$KAFKA_BOOTSTRAP_SERVERS"
```

## 2. Authentication file

`client.properties` is git-ignored. Build it from your environment:

```bash
cat > client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$KAFKA_USERNAME" password="$KAFKA_PASSWORD";
EOF
```

## 3. Topics

```bash
# list
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config client.properties --list

# create the VWAP output topic
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config client.properties \
  --create --topic imat3a-ADA-VWAP --partitions 1 --replication-factor 1

# describe / delete
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config client.properties --describe --topic imat3a_test
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config client.properties --delete   --topic imat3a_test
```

## 4. Produce and consume by hand

```bash
# one keyed message
echo '1,hello' | $KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
  --producer.config client.properties --topic imat3a_test --property parse.key=true --property key.separator=,

# read it back
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
  --consumer.config client.properties --topic imat3a_test --property print.key=true \
  --consumer-property group.id=imat3a_group1
```

## 5. Consumer groups

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --command-config client.properties --list
$KAFKA_HOME/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --command-config client.properties --describe --group imat3a_group1
```
