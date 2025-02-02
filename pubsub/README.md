# Realtime Dashboard
## Redis
```bash
$ redis-server --save "" --io-threads 7 --hz 100
$ go run pubsub/realtime_dashboard/redis/main.go -semaphore=200000 -participants=2000000 -k=20 -durations=20
$ redis-cli shutdown
```

```bash
$ INSTANCES=20 && for port in $(seq 7000 $((7000 + $INSTANCES - 1))); do redis-server --port $port --cluster-enabled yes --cluster-config-file nodes-$port.conf --save "" --hz 100 & sleep 0.2; done
# jobs
$ redis-cli --cluster create $(for port in $(seq 7000 $((7000 + $INSTANCES - 1))); do echo -n "127.0.0.1:$port "; done) --cluster-replicas 1 --cluster-yes
$ go run pubsub/realtime_dashboard/redis/main.go -cluster -instances=$INSTANCES -semaphore=200000 -participants=2000000 -k=20 -durations=20
$ for port in $(seq 7000 $((7000 + $INSTANCES - 1))); do redis-cli -p $port shutdown; done && rm -rf nodes*.conf dump.rdb
```


## Kafka
```sh
$ docker run --name kafka -p 9092:9092 -d apache/kafka-native
$ go run pubsub/realtime_dashboard/kafka/main.go -semaphore=200000 -participants=200000 -k=20 -durations=20
$ docker stop $(docker ps -q -f name=kafka) && docker rm $(docker ps -aq -f name=kafka)
```

```bash
$ export LOCAL_IP_ADDRESS=$(ifconfig | awk '/inet / && !/127.0.0.1/ {print $2; exit}') && docker compose -f docker-compose.kafka.yml up -d --pull=always
$ go run pubsub/realtime_dashboard/kafka/main.go -cluster -semaphore=200000 -participants=200000 -k=20 -durations=20
$ docker compose -f docker-compose.kafka.yml down -v
```



# Stock Exchange
## Redis
```bash
$ redis-server --save "" --io-threads 7 --hz 100
$ go run pubsub/stock_exchange/redis/main.go -participants=2000 -durations=200
$ redis-cli shutdown
```

```bash
$ INSTANCES=20 && for port in $(seq 7000 $((7000 + $INSTANCES - 1))); do redis-server --port $port --cluster-enabled yes --cluster-config-file nodes-$port.conf --save "" --hz 100 & sleep 0.2; done
# jobs
$ redis-cli --cluster create $(for port in $(seq 7000 $((7000 + $INSTANCES - 1))); do echo -n "127.0.0.1:$port "; done) --cluster-replicas 1 --cluster-yes
$ go run pubsub/stock_exchange/redis/main.go -cluster -instances=$INSTANCES -participants=2000 -durations=200
$ for port in $(seq 7000 $((7000 + $INSTANCES - 1))); do redis-cli -p $port shutdown; done && rm -rf nodes*.conf dump.rdb
```


## Kafka
```bash
$ docker run --name kafka -p 9092:9092 -d apache/kafka-native
$ go run pubsub/stock_exchange/kafka/main.go -participants=2000 -durations=10
$ docker stop $(docker ps -q -f name=kafka) && docker rm $(docker ps -aq -f name=kafka)
```

```bash
$ export LOCAL_IP_ADDRESS=$(ifconfig | awk '/inet / && !/127.0.0.1/ {print $2; exit}') && docker compose -f docker-compose.kafka.yml up -d --pull=always
$ go run pubsub/stock_exchange/kafka/main.go -cluster -participants=200 -durations=10
$ docker compose -f docker-compose.kafka.yml down -v
```
