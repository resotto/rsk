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
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

var (
	cluster             bool
	requests, semaphore int
	bits, position      int

	cli []*kgo.Client

	hashFunc      = xxHash{}
	currentFilter *bloomFilter

	queryFailedCounts atomic.Int64

	messageStreams []string
	consumerGroup  = "consumer-group"
	messageKey     = "bytes"
	brokerNodes    = 2
)

func parseFlags() {
	flag.BoolVar(&cluster, "cluster", false, "")
	flag.IntVar(&requests, "requests", 10000, "")
	flag.IntVar(&semaphore, "semaphore", 1000, "")
	flag.IntVar(&bits, "bits", 1_000_000, "")
	flag.IntVar(&position, "position", 6, "")

	flag.Parse()
}

func main() {
	parseFlags()

	currentFilter = newBloomFilter()
	seeds := make([]string, 0)

	if cluster {
		for i := range brokerNodes {
			seeds = append(seeds, "localhost:"+strconv.Itoa(9000+i))
		}
	} else {
		seeds = append(seeds, "localhost:9092")
	}

	ctx := context.Background()

	for i := range brokerNodes {
		cl, err := createClient(seeds, i)
		if err != nil {
			log.Fatal(err.Error())
		}
		cli = append(cli, cl)
		defer cl.Close()
	}

	repairState()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(semaphore)

	for i := range requests {
		eg.Go(func() error { return executeFilter(ctx, i) })
	}

	err := eg.Wait()

	fmt.Println("result (error):", err)
	fmt.Println("cluster:", cluster)
	fmt.Println("requests:", requests)
	fmt.Println("semaphore:", semaphore)
	fmt.Println("bits:", bits)
	fmt.Println("position:", position)
	fmt.Println("query failed counts (should be 0 from the 2nd time onwords with same arguments):", queryFailedCounts.Load())
}

func executeFilter(ctx context.Context, i int) error {
	fmt.Println("executeFilter", i) // debug
	bytes := generateSameKeys(i)
	if !currentFilter.query(bytes) {
		fmt.Println("the filter doesn't have the key", bytes)
		queryFailedCounts.Add(1)
		currentFilter.update(bytes)
		if err := produce(ctx, i%brokerNodes, bytes); err != nil {
			fmt.Println("failed to produce:", err)
		}
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

func createClient(seeds []string, i int) (*kgo.Client, error) {
	stream := "stream" + strconv.Itoa(i)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(stream),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableAutoCommit(), // but we won't ACK messages in order to replay all messages every time you execute this program
		kgo.FetchMaxBytes(100*1024*1024),
		// kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		fmt.Println("failed to create client:", err)
		return nil, err
	}
	fmt.Println("createClient:", consumerGroup)
	messageStreams = append(messageStreams, stream)
	return cl, nil
}

func repairState() {
	for i := range messageStreams {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		fetches := cli[i].PollFetches(ctx)
		if errs := fetches.Errors(); 0 < len(errs) {
			fmt.Println("failed to poll fetches: ", errs)
			continue
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			currentFilter.update(record.Value)
		}
	}
}

func produce(ctx context.Context, i int, bytes []byte) error {
	var res error
	record := &kgo.Record{Topic: messageStreams[i], Key: []byte(messageKey), Value: bytes}
	cli[i].Produce(ctx, record, func(_ *kgo.Record, err error) {
		res = err
	})
	return res
}
