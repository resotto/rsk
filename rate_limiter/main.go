package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	redis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

var tokenBucket = redis.NewScript(`
	local key = KEYS[1]
	local t = redis.call('TIME')
	local now_ms = tonumber(t[1]) * 1000 + tonumber(t[2]) / 1000
	local t_key = key .. '-t'
	local refill_rate = tonumber(ARGV[1])
	local bucket_size = tonumber(ARGV[2])

	local tokens = tonumber(redis.call('GET', key) or 0)
	local last = tonumber(redis.call('GET', t_key) or 0)
	local duration = math.max(0, now_ms - last)
	tokens = math.min(bucket_size, tokens + math.floor(duration / 1000 * refill_rate))

	if tokens == 0 then
		return 0
	end

	redis.call('SET', key, tokens - 1)
	redis.call('SET', t_key, now_ms)
	return 1
`)

var slidingWindowLog = redis.NewScript(`
	local key = KEYS[1]
	local t = redis.call('TIME')
	local now_ms = tonumber(t[1]) * 1000 + tonumber(t[2]) / 1000
	local ttl_ms = tonumber(ARGV[1])
	local threshold = tonumber(ARGV[2])

	redis.call('ZREMRANGEBYSCORE', key, "-inf", now_ms - ttl_ms - 1)
	local count = redis.call('ZCARD', key)

	if count == threshold then
		return 0
	end

	redis.call('ZADD', key, now_ms, now_ms)
	return 1
`)

var (
	cluster                                                               bool
	algorithm                                                             string // "token_bucket" or not
	key                                                                   string = "user_id_1"
	numOfRequest, semaphoreSize, ttlms, threshold, refillRate, bucketSize int
	cli                                                                   redis.Scripter
)

func main() {
	parseFlags()

	runtime.GOMAXPROCS(runtime.NumCPU())

	if cluster {
		cli = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    []string{":7000", ":7001", ":7002", ":7003", ":7004", ":7005"},
			PoolSize: 10000,
		})
	} else {
		cli = redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			PoolSize: 10000,
		})
	}

	var handled, throttled atomic.Int64
	var err error
	startTime := time.Now()
	if algorithm == "token_bucket" {
		handled, throttled, err = execute(tokenBucket, "token-", strconv.Itoa(bucketSize), strconv.Itoa(refillRate))
	} else {
		handled, throttled, err = execute(slidingWindowLog, "sliding-", strconv.Itoa(ttlms), strconv.Itoa(threshold))
	}
	duration := time.Since(startTime)

	printResults(err, duration.Milliseconds(), handled.Load(), throttled.Load())
}

func parseFlags() {
	flag.BoolVar(&cluster, "cluster", false, "")
	flag.StringVar(&algorithm, "algorithm", "token_bucket", "")
	flag.IntVar(&numOfRequest, "numOfRequest", 20, "")
	flag.IntVar(&semaphoreSize, "semaphoreSize", 1, "")
	flag.IntVar(&ttlms, "ttlms", 1000, "")
	flag.IntVar(&threshold, "threshold", 1000, "")
	flag.IntVar(&refillRate, "refillRate", 1000, "")
	flag.IntVar(&bucketSize, "bucketSize", 1000, "")

	flag.Parse()
}

func execute(script *redis.Script, prefix string, args ...interface{}) (handled atomic.Int64, throttled atomic.Int64, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	script.Load(ctx, cli)
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(semaphoreSize)
	keystr := "{" + prefix + key + "}" // hash tag for Redis cluster hash slots
	for i := 0; i < numOfRequest; i++ {
		eg.Go(func() error {
			res, err := script.EvalSha(ctx, cli, []string{keystr}, args).Result()
			if err != nil {
				return nil // ignore error for the performance measurement
			}
			resVal, ok := res.(int64)
			if !ok {
				return fmt.Errorf("unexpected type for res: %T", res)
			} else if resVal == 0 {
				fmt.Println("*", i, "th request is throttled")
				throttled.Add(1)
			} else {
				fmt.Println("*", i, "th request is successfully processed")
				handled.Add(1)
			}
			return nil
		})
	}
	err = eg.Wait()
	return
}

func printResults(err error, duration, handled, throttled int64) {
	fmt.Println("result (error):", err)
	fmt.Println("cluster:", cluster)
	fmt.Println("algorithm:", algorithm)
	fmt.Println("numOfRequest:", numOfRequest)
	fmt.Println("semaphoreSize:", semaphoreSize)
	if algorithm == "token_bucket" {
		fmt.Println("refillRate:", refillRate)
		fmt.Println("bucketSize:", bucketSize)
	} else {
		fmt.Println("ttlms:", ttlms)
		fmt.Println("threshold:", threshold)
	}
	fmt.Println("execution time:", duration, "milliseconds")
	fmt.Println("handled request:", handled)
	fmt.Println("throttled request:", throttled)
	fmt.Println("Redis RPS:", int(float64(handled+throttled)/float64(duration)*1000))
}
