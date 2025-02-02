package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

var (
	cluster                           bool
	publishers, intervalms, durations int

	messageStreams []string
	readCounts     map[string]*atomic.Int64 = make(map[string]*atomic.Int64)
	ackCounts      map[string]*atomic.Int64 = make(map[string]*atomic.Int64)

	cli []*kgo.Client

	consumerGroup          = "consumer-group"
	brokerNodes, partition = 2, 2 // num of broker nodes, KAKFA_NUM_PARTITIONS
)

func parseFlags() {
	flag.BoolVar(&cluster, "cluster", false, "")
	flag.IntVar(&publishers, "publishers", 1000, "")
	flag.IntVar(&intervalms, "intervalms", 1000, "")
	flag.IntVar(&durations, "durations", 10, "")

	flag.Parse()
}

func main() {
	parseFlags()

	seeds := make([]string, 0)

	if cluster {
		for i := range brokerNodes {
			seeds = append(seeds, "localhost:"+strconv.Itoa(9000+i))
		}
	} else {
		brokerNodes = 1
		partition = 1
		seeds = append(seeds, "localhost:9092")
	}

	for i := range brokerNodes {
		cl, err := createClient(seeds, i)
		if err != nil {
			log.Fatal(err.Error())
		}
		cli = append(cli, cl)
		defer cl.Close()
	}

	eg, ctx := errgroup.WithContext(context.Background())

	for i := range publishers {
		eg.Go(func() error { return produce(ctx, i%brokerNodes) })
	}

	if err := eg.Wait(); err != nil {
		log.Fatal(err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durations)*time.Second)
	defer cancel()

	eg, ctx = errgroup.WithContext(ctx)

	for i := range brokerNodes * partition {
		eg.Go(func() error { return consume(ctx, i%brokerNodes) })
	}

	err := eg.Wait()

	fmt.Println("result (error):", err.Error())
	if cluster {
		fmt.Println("cluster:", cluster)
	}
	fmt.Println("publishers:", publishers)
	fmt.Println("intervalms:", intervalms)
	fmt.Println("durations:", durations)
	for i := range brokerNodes {
		stream := getName("stream", i)
		fmt.Println(stream, "readCounts", readCounts[stream].Load(), "ackCounts", ackCounts[stream].Load())
	}
}

func createClient(seeds []string, i int) (*kgo.Client, error) {
	stream := getName("stream", i)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(stream),
		kgo.AllowAutoTopicCreation(),
		// kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		fmt.Println("failed to create client:", err)
		return nil, err
	}
	fmt.Println("create client", stream)
	messageStreams = append(messageStreams, stream)
	readCounts[stream] = &atomic.Int64{}
	ackCounts[stream] = &atomic.Int64{}
	return cl, nil
}

func produce(ctx context.Context, i int) error {
	var res error
	key, val := []byte(getName("publisher", i)), []byte(strconv.Itoa(i))
	record := &kgo.Record{Topic: messageStreams[i], Key: key, Value: val}
	cli[i].Produce(ctx, record, func(_ *kgo.Record, err error) {
		res = err
	})
	if res != nil {
		fmt.Println("failed to produce:", res)
	} else {
		fmt.Println("produce:", res)
	}
	return res
}

func consume(ctx context.Context, i int) error {
	stream := messageStreams[i]
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			fetches := cli[i].PollFetches(ctx)
			if errs := fetches.Errors(); 0 < len(errs) {
				fmt.Println("failed to poll fetches: ", errs)
				continue
			}
			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				fmt.Println("consume:", record)
				readCounts[stream].Add(1)
				time.Sleep(time.Duration(intervalms) * time.Millisecond)
			}
		}
	}
}

func getName(prefix string, i int) string {
	return prefix + "-" + strconv.Itoa(i)
}
