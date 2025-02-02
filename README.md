(This repository is based on Redis 7.2)

# Use cases
1. [Rate Limiter](./rate_limiter/README.md)
1. [Application Cache](./application_cache/README.md)
1. [PubSub](./pubsub/README.md)
1. [Streams](./streams/README.md)

# Prerequisites
```bash
$ git clone git@github.com:resotto/rsk.git
$ cd rsk
# install Go 1.23~ if necessary
$ go mod tidy
# brew install redis # if necessary
# install docker compose # if necessary
```

# Machine spec
| Hardware | OS | Processor | Memory |
|:-:|:-:|:-:|:-:|
| Macbook Pro 2019 | Sequoia 15.0.1 | 2.3 GHz 8-Core Intel Core i9 | 32 GB 2667 MHz DDR4 |
