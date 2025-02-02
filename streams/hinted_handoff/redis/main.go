package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	xxhashv2 "github.com/cespare/xxhash/v2"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

var (
	cluster                        bool
	instances, requests, semaphore int
	bits, position                 int

	cli redis.UniversalClient

	hashFunc      = xxHash{}
	currentFilter *bloomFilter

	queryFailedCounts atomic.Int64

	messageStreams []string
	consumerGroup  = "consumer-group"
	consumer       = "consumer"
	messageKey     = "bytes"
)

func parseFlags() {
	flag.BoolVar(&cluster, "cluster", false, "")
	flag.IntVar(&instances, "instances", 1, "")
	flag.IntVar(&requests, "requests", 10000, "")
	flag.IntVar(&semaphore, "semaphore", 1000, "")
	flag.IntVar(&bits, "bits", 1_000_000, "")
	flag.IntVar(&position, "position", 6, "")

	flag.Parse()
}

func main() {
	parseFlags()

	currentFilter = newBloomFilter()

	if cluster {
		addrs := make([]string, 0)
		for i := range instances {
			addrs = append(addrs, ":"+strconv.Itoa(7000+i))
		}
		cli = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: addrs,
		})
	} else {
		cli = redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})
	}

	ctx := context.Background()

	for i := range instances {
		if err := createGroup(ctx, i); err != nil {
			log.Fatal(err.Error())
		}
	}

	if err := repairState(ctx); err != nil {
		log.Fatal(err.Error())
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(semaphore)

	for i := range requests {
		eg.Go(func() error { return executeFilter(ctx, i) })
	}

	err := eg.Wait()

	for i := range instances {
		if _, err := cli.XGroupDestroy(context.Background(), messageStreams[i], consumerGroup).Result(); err != nil {
			log.Fatal(err.Error())
		}
	}

	fmt.Println("result (error):", err)
	fmt.Println("cluster:", cluster)
	fmt.Println("requests:", requests)
	fmt.Println("semaphore:", semaphore)
	fmt.Println("bits:", bits)
	fmt.Println("position:", position)
	fmt.Println("query failed counts (should be 0 from the 2nd time onwords with same arguments):", queryFailedCounts.Load())
}

func executeFilter(ctx context.Context, i int) error {
	bytes := generateSameKeys(i)
	if !currentFilter.query(bytes) {
		fmt.Println("the filter doesn't have the key", bytes)
		queryFailedCounts.Add(1)
		currentFilter.update(bytes)
		xAdd(ctx, i%instances, bytes)
	}
	return nil
}

func generateSameKeys(i int) []byte {
	return []byte(strconv.Itoa(i))
}

type xxHash struct{}

func (x xxHash) hash(bytes []byte) uint64 {
	return xxhashv2.Sum64(bytes)
}

type bloomFilter struct {
	arr []bool
}

func newBloomFilter() *bloomFilter {
	return &bloomFilter{arr: make([]bool, bits)}
}

func (f *bloomFilter) update(bytes []byte) {
	f.getPos(bytes, true)
}

func (f *bloomFilter) query(bytes []byte) bool {
	return f.getPos(bytes, false)
}

func (f *bloomFilter) getPos(bytes []byte, set bool) bool {
	for i := range position {
		if i%256 == 0 {
			bytes = append(bytes, 0)
		} else {
			bytes[len(bytes)-1]++
		}
		j := int(hashFunc.hash(bytes) % uint64(bits))
		if set {
			f.arr[j] = true
		} else if !f.arr[j] {
			return false
		}
	}
	return true
}

func createGroup(ctx context.Context, i int) error {
	stream := "stream" + strconv.Itoa(i)
	res, err := cli.XGroupCreateMkStream(ctx, stream, consumerGroup, "0").Result()
	if err != nil {
		fmt.Println("failed to XGroupCreateMKStream:", err, res)
		return err
	}
	fmt.Println("XGroupCreateMKSream:", consumerGroup, stream, res)
	messageStreams = append(messageStreams, stream)
	return nil
}

func repairState(ctx context.Context) error {
	for _, stream := range messageStreams {
		res, err := cli.XGroupCreateConsumer(ctx, stream, consumerGroup, consumer).Result()
		if err != nil {
			fmt.Println("failed to XGroupCreateConsumer:", err)
			return err
		}
		fmt.Println("XGroupCreateConsumer:", res)

		count, err := cli.XLen(ctx, stream).Result()
		if err != nil {
			fmt.Println("failed to XLen for", stream)
			continue
		} else if count == 0 {
			fmt.Println(stream, "doesn't have any messages yet")
			continue
		}

		streams := []string{stream, ">"}
		for {
			res, err := xReadGroup(ctx, consumerGroup, consumer, streams)
			if err != nil {
				fmt.Println("failed to XReadGroup:", res, err, streams, consumerGroup, consumer)
				break
			} else {
				fmt.Println("XReadGroup:", res)
				if len(res[0].Messages) == 0 {
					break
				}
				for _, m := range res[0].Messages {
					v, ok := m.Values[messageKey].(string)
					if !ok {
						fmt.Println("failed to cast the value", v, "associated with key", messageKey)
						continue
					}
					currentFilter.update([]byte(v))
				}
				streams[1] = res[0].Messages[len(res[0].Messages)-1].ID
			}
		}
	}
	return nil
}

func xReadGroup(ctx context.Context, group, consumer string, streams []string) ([]redis.XStream, error) {
	param := redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  streams,
		Count:    1_000_000,
		Block:    200 * time.Millisecond,
		NoAck:    false, // but we won't ACK messages in order to replay all messages every time you execute this program
	}
	return cli.XReadGroup(ctx, &param).Result()
}

func xAdd(ctx context.Context, i int, bytes []byte) {
	param := redis.XAddArgs{
		Stream:     messageStreams[i],
		NoMkStream: false,
		Values:     []string{messageKey, string(bytes)},
	}
	res, err := cli.XAdd(ctx, &param).Result()
	if err != nil {
		fmt.Println("failed to XAdd:", err)
	} else {
		fmt.Println("XAdd:", res)
	}
}
