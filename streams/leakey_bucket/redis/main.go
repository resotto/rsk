package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

var (
	cluster                                                                      bool
	instances, publishers, subscribers, evictedOldEntries, intervalms, durations int

	readGroupCounts atomic.Int64
	autoclaimCounts atomic.Int64

	r              *rand.Rand
	hundredPercent = 10_000

	cli           redis.UniversalClient
	stream        = "stream"
	consumerGroup = "consumer-group"
)

func parseFlags() {
	flag.BoolVar(&cluster, "cluster", false, "")
	flag.IntVar(&instances, "instances", 1, "")
	flag.IntVar(&publishers, "publishers", 1000, "")
	flag.IntVar(&subscribers, "subscribers", 10, "")
	flag.IntVar(&evictedOldEntries, "evicted_old_entries", 0, "")
	flag.IntVar(&intervalms, "intervalms", 1000, "")
	flag.IntVar(&durations, "durations", 10, "")

	flag.Parse()
}

func main() {
	parseFlags()

	r = rand.New(rand.NewSource(time.Now().UnixMicro()))

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

	if err := createGroup(ctx); err != nil {
		log.Fatal(err.Error())
	}

	eg, ctx := errgroup.WithContext(ctx)

	for i := range publishers {
		eg.Go(func() error { return xAdd(ctx, i) })
	}

	if err := eg.Wait(); err != nil {
		log.Fatal(err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durations)*time.Second)
	defer cancel()

	eg, ctx = errgroup.WithContext(ctx)

	for i := range subscribers {
		eg.Go(func() error { return consume(ctx, i) })
	}

	err := eg.Wait()

	if err := cleanup(); err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("result (error):", err.Error())
	if cluster {
		fmt.Println("cluster:", cluster)
		fmt.Println("instances:", instances)
	}
	fmt.Println("publishers:", publishers)
	fmt.Println("subscribers:", subscribers)
	fmt.Println("evictedOldEntries:", evictedOldEntries)
	fmt.Println("intervalms:", intervalms)
	fmt.Println("durations:", durations)
	fmt.Println(stream, "XReadGroup", readGroupCounts.Load(), "XAutoClaim", autoclaimCounts.Load())
}

func createGroup(ctx context.Context) error {
	res, err := cli.XGroupCreateMkStream(ctx, stream, consumerGroup, "0").Result()
	if err != nil {
		fmt.Println("failed to XGroupCreateMKStream:", err, res)
		return err
	}
	fmt.Println("XGroupCreateMKSream:", consumerGroup, res)
	return nil
}

func xAdd(ctx context.Context, i int) error {
	param := redis.XAddArgs{
		Stream:     stream,
		NoMkStream: false,
		MaxLen:     int64(evictedOldEntries),
		Approx:     true,
		Values:     []string{getName("publisher", i), strconv.Itoa(i)},
	}
	res, err := cli.XAdd(ctx, &param).Result()
	if err != nil {
		fmt.Println("failed to XAdd:", err)
	} else {
		fmt.Println("XAdd:", res)
	}
	return nil
}

func consume(ctx context.Context, i int) error {
	idx := i % instances
	consumer := getName("consumer", idx)
	res, err := cli.XGroupCreateConsumer(ctx, stream, consumerGroup, consumer).Result()
	if err != nil {
		fmt.Println("failed to XGroupCreateConsumer:", err)
		return err
	}
	fmt.Println("XGroupCreateConsumer:", res)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			xReadGroup(ctx, consumerGroup, consumer, stream)
			if 1 <= xPending(ctx) {
				if 0 < xAutoClaim(ctx, stream, consumerGroup, "0-0", consumer, 1) {
					xReadGroup(ctx, consumerGroup, consumer, stream)
				}
			}
			time.Sleep(time.Duration(intervalms) * time.Millisecond)
		}
	}
}

func xPending(ctx context.Context) int64 {
	res, err := cli.XPending(ctx, stream, consumerGroup).Result()
	if err != nil {
		fmt.Println("failed to XPending:", err)
		return 0
	}
	return res.Count
}

func xReadGroup(ctx context.Context, group, consumer, stream string) {
	param := redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    1 * time.Nanosecond,
		NoAck:    false,
	}
	res, err := cli.XReadGroup(ctx, &param).Result()
	if err != nil {
		fmt.Println("failed to XReadGroup:", err, stream, group, consumer)
	} else {
		fmt.Println("XReadGroup:", res)
		readGroupCounts.Add(int64(len(res)))
		xAck(ctx, res, stream, group)
	}
}

func xAck(ctx context.Context, res []redis.XStream, stream, group string) {
	for _, v := range res {
		ids := make([]string, len(v.Messages))
		for i, m := range v.Messages {
			ids[i] = m.ID
		}
		if r.Intn(hundredPercent) < 2000 {
			continue // Nack with 20% probability intentionally
		}
		_, err := cli.XAck(ctx, stream, group, ids...).Result()
		if err != nil {
			fmt.Println("failed to XACK", err)
		}
	}
}

func xAutoClaim(ctx context.Context, stream, group, start, consumer string, count int64) int {
	param := redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		MinIdle:  1 * time.Millisecond,
		Start:    start,
		Count:    count,
		Consumer: consumer,
	}
	res, start, err := cli.XAutoClaim(ctx, &param).Result()
	if err != nil {
		fmt.Println("failed to XAutoClaim:", err, stream, group, consumer)
		return 0
	}
	fmt.Println("XAutoClaim for:", stream, "by", consumer, "with response:", res, start)
	autoclaimCounts.Add(int64(len(res)))
	return len(res)
}

func cleanup() error {
	_, err := cli.XGroupDestroy(context.Background(), stream, consumerGroup).Result()
	if err != nil {
		fmt.Println("failed to XGroupDestroy", err)
	}
	_, err = cli.Del(context.Background(), stream).Result()
	if err != nil {
		fmt.Println("failed to Del stream", err)
	}
	return err
}

func getName(prefix string, i int) string {
	return prefix + "-" + strconv.Itoa(i)
}
