package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	xxhashv2 "github.com/cespare/xxhash/v2"
	"github.com/gocql/gocql"
	farmhash "github.com/leemcloughlin/gofarmhash"
	"github.com/scylladb/gocqlx/table"
	"github.com/scylladb/gocqlx/v3"
	"golang.org/x/sync/errgroup"
)

type Timestamps struct {
	TsHash string `json:"ts_hash" cassandra:"ts_hash"`
	Ts     string `json:"ts" cassandra:"ts"`
}

var (
	isCluster           bool
	algorithm           string // bloom_filter or not
	requests, semaphore int
	hash                string // xx, farm
	length              int    // for RFilter
	bits, position      int    // for BloomFilter

	timestampsTable *table.Table

	hashFunc      hashFunction
	currentFilter filter

	memStatsBefore, memStatsAfter                                   runtime.MemStats
	truePositive, trueNegative, falsePositive, falseNegative, other atomic.Int64
	syncMap                                                         sync.Map
)

func parseFlags() {
	flag.BoolVar(&isCluster, "cluster", false, "")
	flag.StringVar(&algorithm, "algorithm", "bloom_filter", "")
	flag.IntVar(&requests, "requests", 10000, "")
	flag.IntVar(&semaphore, "semaphore", 1000, "")
	flag.StringVar(&hash, "hash", "xx", "")
	flag.IntVar(&length, "length", 6, "")
	flag.IntVar(&bits, "bits", 1_000_000, "")
	flag.IntVar(&position, "position", 6, "")

	flag.Parse()
}

func main() {
	parseFlags()

	hosts := []string{"localhost:9042"}
	if isCluster {
		hosts = append(hosts, "localhost:9043")
	}
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = "example"
	cluster.NumConns = 4
	cluster.Consistency = gocql.LocalOne
	cluster.Timeout = 1 * time.Minute
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: 3,
		Min:        3 * time.Second,
		Max:        20 * time.Second,
	}

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		panic("failed to connect to cluster:" + err.Error())
	}
	defer session.Close()

	createTableQuery := `
		CREATE TABLE IF NOT EXISTS example.timestamps (
			ts_hash TEXT,
			ts TEXT,
			PRIMARY KEY (ts_hash, ts)
		);
	`
	if err := session.Query(createTableQuery, nil).Exec(); err != nil {
		panic("failed to create table:" + err.Error())
	}

	timestampsMetadata := table.Metadata{
		Name:    "timestamps",
		Columns: []string{"ts_hash", "ts"},
		PartKey: []string{"ts_hash"},
		SortKey: []string{"ts"},
	}
	timestampsTable = table.New(timestampsMetadata)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(semaphore)

	if hash == "xx" {
		hashFunc = xxHash{}
	} else {
		hashFunc = farmHash{}
	}

	if algorithm == "bloom_filter" {
		currentFilter = newBloomFilter()
	} else {
		currentFilter = newRFilter()
	}

	runtime.GC()
	runtime.ReadMemStats(&memStatsBefore)
	startTime := time.Now()

	for i := 0; i < requests; i++ {
		eg.Go(func() error { return executeFilter(i, &session) })
	}

	err = eg.Wait()
	duration := time.Since(startTime)
	runtime.GC()
	runtime.ReadMemStats(&memStatsAfter)
	total := truePositive.Load() + trueNegative.Load() + falsePositive.Load() + falseNegative.Load() + other.Load()

	fmt.Println("result (error):", err)
	fmt.Println("cluster:", isCluster)
	fmt.Println("algorithm:", algorithm)
	fmt.Println("requests:", requests)
	fmt.Println("semaphore:", semaphore)
	fmt.Println("hash:", hash)
	if algorithm == "bloom_filter" {
		fmt.Println("bits:", bits)
		fmt.Println("position:", position)
	} else {
		fmt.Println("length:", length)
	}
	fmt.Println("execution time:", duration.Milliseconds(), "milliseconds")
	if algorithm == "bloom_filter" {
		arr := currentFilter.array()
		fmt.Println("used memory:", (unsafe.Sizeof(arr)+uintptr(len(arr))*unsafe.Sizeof(arr[0]))/1024/1024, "MB")
	} else {
		fmt.Println("used memory:", (memStatsAfter.Alloc-memStatsBefore.Alloc)/1024/1024, "MB")
	}
	fmt.Println("true positive:", truePositive.Load(), "(", float64(truePositive.Load())/float64(total)*100, "%)")
	fmt.Println("true negative:", trueNegative.Load(), "(", float64(trueNegative.Load())/float64(total)*100, "%)")
	fmt.Println("false positive:", falsePositive.Load(), "(", float64(falsePositive.Load())/float64(total)*100, "%)")
	fmt.Println("false negative:", falseNegative.Load(), "(", float64(falseNegative.Load())/float64(total)*100, "%)")
	fmt.Println("other:", other.Load(), "(", float64(other.Load())/float64(total)*100, "%)")
	fmt.Println("RPS:", int(float64(total)/float64(duration.Milliseconds())*1000))
}

func executeFilter(i int, session *gocqlx.Session) error {
	orig := getSimilarKey()
	bytes := currentFilter.convert(orig)
	str := string(orig)
	mu := getMutex(str)
	mu.Lock() // mutex used for measuring ability of the filter
	defer mu.Unlock()
	t := Timestamps{
		TsHash: str,
		Ts:     string(bytes),
	}
	if i%2 == 0 {
		t, err := insert(session, &t)
		if err != nil {
			other.Add(1)
			return nil // ignore error when inserting value
		}
		fmt.Println(i, "th database insert succeeded with:", t)
		currentFilter.update(bytes)
		return nil
	}
	result := currentFilter.query(bytes)
	if err := session.Query(timestampsTable.Get()).BindStruct(t).GetRelease(&Timestamps{}); err != nil {
		if err == gocql.ErrNotFound {
			if result {
				falsePositive.Add(1) // filter expects the data but it actually doesn't exist
			} else {
				trueNegative.Add(1) // filter doesn't expect the data and it actually doesn't exist
			}
		} else {
			fmt.Println("other error happend on database select", err.Error())
		}
	} else {
		if result {
			truePositive.Add(1) // filter expects the data and it actually exists
		} else {
			falseNegative.Add(1) // filter doens't expect the data but it actually exists
		}
	}
	return nil
}

func insert(session *gocqlx.Session, t *Timestamps) (*Timestamps, error) {
	if err := session.Query(timestampsTable.Get()).BindStruct(t).GetRelease(&Timestamps{}); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			if err := session.Query(timestampsTable.Insert()).BindStruct(t).ExecRelease(); err != nil {
				// omit retry logic for convenience but possibly already exists error
				return nil, fmt.Errorf("failed to insert data into database: %v", err)
			}
		} else {
			// omit retry logic for convenience
			return nil, fmt.Errorf("failed to query database: %v", err)
		}
	} else {
		fmt.Println("row found when inserting value (AlreadyExists)")
	}
	return t, nil
}

func getSimilarKey() []byte {
	t := time.Now().UnixMicro()
	return []byte(fmt.Sprintf("%d", t))
}

func getMutex(s string) *sync.Mutex {
	mu, _ := syncMap.LoadOrStore(s, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

type hashFunction interface {
	hash([]byte) uint64
}

type xxHash struct{}

func (x xxHash) hash(bytes []byte) uint64 {
	return xxhashv2.Sum64(bytes)
}

type farmHash struct{}

func (f farmHash) hash(bytes []byte) uint64 {
	return farmhash.Hash64(bytes)
}

type filter interface {
	update([]byte)
	query([]byte) bool
	convert([]byte) []byte
	array() []bool
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

func (f *bloomFilter) convert(bytes []byte) []byte {
	return bytes
}

func (f *bloomFilter) getPos(bytes []byte, set bool) bool {
	for i := 0; i < position; i++ {
		if i%256 == 0 {
			bytes = append(bytes, 0)
		} else {
			bytes[len(bytes)-1]++
		}
		val := hashFunc.hash(bytes)
		j := int(val % uint64(bits))
		if set {
			f.arr[j] = true
		} else if !f.arr[j] {
			return false
		}
	}
	return true // can be false positive
}

func (f *bloomFilter) array() []bool { return f.arr }

type rFilter struct {
	next []*rFilter
}

func newRFilter() *rFilter {
	return &rFilter{next: make([]*rFilter, 62)}
}

func (r *rFilter) update(bytes []byte) {
	r.getPos(bytes, true)
}

func (r *rFilter) query(bytes []byte) bool {
	return r.getPos(bytes, false)
}

func (r *rFilter) array() []bool { return []bool{} }

func (r *rFilter) getPos(bytes []byte, set bool) bool {
	for _, b := range bytes {
		j := decodeBase62(b)
		if r.next[j] == nil {
			if set {
				r.next[j] = newRFilter()
			} else {
				return false
			}
		}
		r = r.next[j]
	}
	return true
}

func (r *rFilter) convert(bytes []byte) []byte {
	return convertBase62(hashFunc.hash(bytes))
}

func convertBase62(num uint64) []byte {
	bytes := make([]byte, length)
	for i := 0; i < length; i++ {
		mod := num % 62
		num /= 62
		bytes[length-1-i] = encodeBase62(mod)
	}
	return bytes
}

func encodeBase62(b uint64) byte {
	if b < 10 {
		return byte('0' + b)
	} else if b < 36 {
		return byte('A' + b - 10)
	}
	return byte('a' + b - 36)
}

func decodeBase62(b byte) int {
	if '0' <= b && b <= '9' {
		return int(b - '0')
	} else if 'A' <= b && b <= 'Z' {
		return int(b - 'A' + 10)
	}
	return int(b - 'a' + 36)
}
