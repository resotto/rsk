# Hinted Handoff
## Redis
```bash
$ redis-server --save "" --io-threads 7 --hz 100
$ go run streams/hinted_handoff/redis/main.go -requests=100000 -semaphore=1000 -bits=1000000 -position=8
$ go run streams/hinted_handoff/redis/main.go -requests=100000 -semaphore=1000 -bits=1000000 -position=8
$ redis-cli shutdown
```

```bash
$ INSTANCES=20 && for port in $(seq 7000 $((7000 + $INSTANCES - 1))); do redis-server --port $port --cluster-enabled yes --cluster-config-file nodes-$port.conf --save "" --hz 100 & sleep 0.2; done && redis-cli --cluster create $(for port in $(seq 7000 $((7000 + $INSTANCES - 1))); do echo -n "127.0.0.1:$port "; done) --cluster-replicas 1 --cluster-yes
$ go run streams/hinted_handoff/redis/main.go -cluster -instances=$INSTANCES -requests=100000 -semaphore=1000 -bits=1000000 -position=8
$ go run streams/hinted_handoff/redis/main.go -cluster -instances=$INSTANCES -requests=100000 -semaphore=1000 -bits=1000000 -position=8
$ for port in $(seq 7000 $((7000 + $INSTANCES - 1))); do redis-cli -p $port shutdown; done && rm -rf nodes*.conf dump.rdb
```


## Kafka
```bash
$ docker run --name kafka -p 9092:9092 -d apache/kafka-native
$ go run streams/hinted_handoff/kafka/main.go -requests=100000 -semaphore=1000 -bits=1000000 -position=8
$ go run streams/hinted_handoff/kafka/main.go -requests=100000 -semaphore=1000 -bits=1000000 -position=8
$ docker stop $(docker ps -q -f name=kafka) && docker rm $(docker ps -aq -f name=kafka)
```

```bash
$ export LOCAL_IP_ADDRESS=$(ifconfig | awk '/inet / && !/127.0.0.1/ {print $2; exit}') && docker compose -f docker-compose.kafka.yml up -d --pull=always
$ go run streams/hinted_handoff/kafka/main.go -cluster -requests=100000 -semaphore=1000 -bits=1000000 -position=8
$ go run streams/hinted_handoff/kafka/main.go -cluster -requests=100000 -semaphore=1000 -bits=1000000 -position=8
$ docker compose -f docker-compose.kafka.yml down -v
```



# Leakey Bucket
## Redis
```bash
$ redis-server --save "" --io-threads 7 --hz 100
$ go run streams/leakey_bucket/redis/main.go -publishers=100 -subscribers=1 -intervalms=1000
$ redis-cli shutdown
```

```bash
$ INSTANCES=20 && for port in $(seq 7000 $((7000 + $INSTANCES - 1))); do redis-server --port $port --cluster-enabled yes --cluster-config-file nodes-$port.conf --save "" --hz 100 & sleep 0.2; done && redis-cli --cluster create $(for port in $(seq 7000 $((7000 + $INSTANCES - 1))); do echo -n "127.0.0.1:$port "; done) --cluster-replicas 1 --cluster-yes
$ go run streams/leakey_bucket/redis/main.go -cluster -instances=$INSTANCES -publishers=100000 -subscribers=$INSTANCES -intervalms=1000
$ for port in $(seq 7000 $((7000 + $INSTANCES - 1))); do redis-cli -p $port shutdown; done && rm -rf nodes*.conf dump.rdb
```


## Kafka
```bash
$ docker run --name kafka -p 9092:9092 -d apache/kafka-native
$ go run streams/leakey_bucket/kafka/main.go -publishers=100000 -intervalms=1000 -durations=20
$ docker stop $(docker ps -q -f name=kafka) && docker rm $(docker ps -aq -f name=kafka)
```

```bash
$ export LOCAL_IP_ADDRESS=$(ifconfig | awk '/inet / && !/127.0.0.1/ {print $2; exit}') && docker compose -f docker-compose.kafka.yml up -d --pull=always
$ go run streams/leakey_bucket/kafka/main.go -cluster -publishers=100000 -intervalms=1000 -durations=20
$ docker compose -f docker-compose.kafka.yml down -v
```
