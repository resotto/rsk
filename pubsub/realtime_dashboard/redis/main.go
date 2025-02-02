package main

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"sync"
	"time"

	"math/rand"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

const (
	scoreChannel   = "score_channel"
	hundredPercent = 10_000
)

var (
	cluster                    bool
	instances                  int
	semaphore                  int
	participants, k, durations int
	cli                        redis.UniversalClient
	problemOrders              [][]int
	probabilities              = []int{7000, 9500, 9995, 9999}
	ability                    = rand.Intn(100)
	points                     = []int{1, 2, 4, 8}
	topScoreList               = make([]TopScore, 1<<4)
	mutexList                  = make([]sync.Mutex, 1<<4)
	words                      []string
)

func parseFlags() {
	flag.BoolVar(&cluster, "cluster", false, "")
	flag.IntVar(&instances, "instances", -1, "")
	flag.IntVar(&semaphore, "semaphore", 100_000, "")
	flag.IntVar(&participants, "participants", 1_000_000, "")
	flag.IntVar(&k, "k", 20, "")
	flag.IntVar(&durations, "durations", 20, "")

	flag.Parse()
}

func loadWords() {
	file, _ := os.Open("/usr/share/dict/words")
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		words = append(words, scanner.Text())
	}
}

func main() {
	parseFlags()

	if semaphore <= 15 {
		log.Fatal("semaphore is too small to process 15 subscribers")
	}

	rand.New(rand.NewSource(time.Now().UnixMicro()))
	loadWords()

	permutation(0, 0, make([]int, 4))

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

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durations)*time.Second)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(semaphore)

	for i := 1; i < (1 << 4); i++ {
		ts := TopScore{}
		heap.Init(&ts)
		topScoreList[i] = ts
		mutexList[i] = sync.Mutex{}
		eg.Go(func() error { return subscribe(ctx, scoreChannel+strconv.Itoa(i)) })
	}

	eg.Go(func() error { return print(ctx) })

	for i := range participants {
		eg.Go(func() error { return publish(ctx, i) })
	}

	err := eg.Wait()
	if err != context.DeadlineExceeded {
		fmt.Println("result (error):", err.Error())
	}
	if cluster {
		fmt.Println("cluster:", cluster)
		fmt.Println("instances:", instances)
	}
	fmt.Println("semaphore:", semaphore)
	fmt.Println("participants:", participants)
	fmt.Println("k:", k)
	fmt.Println("durations:", durations)
}

type FinishTime struct {
	Abs     int64 `json:"abs"`
	Delta   int64 `json:"delta"`
	Penalty int64 `json:"penalty"`
}

func newFinishTime(abs, delta, penalty int64) FinishTime {
	return FinishTime{Abs: abs, Delta: delta, Penalty: penalty}
}

func hms(total int64) string {
	if total == 0 {
		return ""
	}
	return fmt.Sprintf("%02d:%02d:%02d", total/3600, (total%3600)/60, total%60)
}

func count(s int64) string {
	if s == 0 {
		return ""
	}
	return fmt.Sprintf("(%v)", s)
}

func trimSuffix(s string, width int) string {
	if len(s) <= width {
		return s
	}
	return s[:width-1] + "…"
}

type Score struct {
	ID            int          `json:"id"`
	Name          string       `json:"name"`
	Point         int          `json:"point"`
	Penalty       int64        `json:"penalty"`
	TotalPenalty  int64        `json:"total_penalty"`
	TotalDuration int64        `json:"total_duration"`
	LastTimestamp time.Time    `json:"last_timestamp"`
	FinishTimes   []FinishTime `json:"finish_times"`
}

func newScore(id int, start time.Time) Score {
	rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	sep := []string{"-", " ", "_"}[rand.Intn(3)]
	return Score{
		ID:            id,
		Name:          words[rand.Intn(len(words))] + sep + words[rand.Intn(len(words))] + sep + words[rand.Intn(len(words))],
		FinishTimes:   make([]FinishTime, 4),
		LastTimestamp: start,
	}
}

func (s *Score) update(p int, start time.Time) {
	s.Point += points[p]
	now := time.Now()
	s.TotalPenalty += s.Penalty
	abs := int64(now.Unix() - start.Unix() + s.TotalPenalty*5*60)
	delta := int64(now.Unix() - s.LastTimestamp.Unix() + s.Penalty*5*60)
	s.TotalDuration = abs
	s.FinishTimes[p] = newFinishTime(abs, delta, s.Penalty)
	s.LastTimestamp = now
	s.Penalty = 0
}

func (s *Score) serialize() (string, error) {
	bytes, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func deserialize(p string) (*Score, error) {
	var s Score
	if err := json.Unmarshal([]byte(p), &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func sortScore(i, j Score, rev bool) bool {
	if i.Point == j.Point {
		if rev {
			return i.TotalDuration > j.TotalDuration
		}
		return i.TotalDuration < j.TotalDuration
	}
	if rev {
		return i.Point < j.Point
	}
	return i.Point > j.Point
}

func updateTopScore(s *Score) {
	mutexList[s.Point].Lock()
	topScore := topScoreList[s.Point]
	heap.Push(&topScore, s)
	if k < topScore.Len() {
		heap.Pop(&topScore)
	}
	topScoreList[s.Point] = topScore
	mutexList[s.Point].Unlock()
}

func print(ctx context.Context) error {
	contestName := strconv.Itoa(rand.Intn(1000))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			getTopKScore().print(contestName)
			time.Sleep(time.Duration(durations*1000/10) * time.Millisecond)
		}
	}
}

func subscribe(ctx context.Context, channel string) error {
	ch := cli.SSubscribe(ctx, channel).Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-ch:
			if message.Payload != "ping" {
				s, err := deserialize(message.Payload)
				if err != nil {
					fmt.Println("message deserialization failed: ", err.Error())
				} else {
					updateTopScore(s)
				}
			}
		}
	}
}

func publish(ctx context.Context, i int) error {
	j := 0
	problemOrder := problemOrders[rand.Intn(len(problemOrders))]
	start := time.Now()
	score := newScore(i, start)
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()
	time.Sleep(time.Second)
	for {
		select {
		case <-ctx.Done():
			fmt.Println("closing", i, "th participant's publisher")
			return ctx.Err()
		case <-ticker.C:
			if j == 4 {
				break
			}
			p := problemOrder[j]
			if probabilities[p] <= ability+rand.Intn(hundredPercent) {
				score.update(p, start)
				message, err := score.serialize()
				if err != nil {
					fmt.Println("message serialization failed: ", err.Error())
				} else if _, err := cli.SPublish(ctx, scoreChannel+strconv.Itoa(score.Point), message).Result(); err != nil && err != context.DeadlineExceeded {
					fmt.Println(i, " th participant failed to publish score with point: ", score.Point, err.Error())
				}
				j++
			} else {
				score.Penalty++
			}
		}
	}
}

func permutation(i, mask int, s []int) {
	if i == 4 {
		problemOrders = append(problemOrders, []int{s[0], s[1], s[2], s[3]})
		return
	}
	for j := range 4 {
		if (mask & (1 << j)) == 0 {
			s[i] = j
			permutation(i+1, mask^(1<<j), s)
			s[i] = 0
		}
	}
}

type TopScore []*Score

func (ts TopScore) Len() int { return len(ts) }

func (ts TopScore) Less(i, j int) bool {
	return sortScore(*ts[i], *ts[j], true)
}

func (ts TopScore) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}

func (ts *TopScore) Push(x any) {
	s := x.(*Score)
	*ts = append(*ts, s)
}

func (ts *TopScore) Pop() any {
	old := *ts
	n := len(old)
	s := old[n-1]
	old[n-1] = nil
	*ts = old[0 : n-1]
	return s
}

func getTopKScore() TopScore {
	topKScore := make(TopScore, 0)
	for i := (1 << 4) - 1; 0 <= i; i-- {
		mutexList[i].Lock()
		topKScore = append(topKScore, topScoreList[i]...)
		mutexList[i].Unlock()
		if k <= len(topKScore) {
			topKScore = topKScore[:k]
			break
		}
	}
	sort.Slice(topKScore, func(i, j int) bool {
		return sortScore(*topKScore[i], *topKScore[j], false)
	})
	return topKScore
}

func (ts TopScore) print(contestName string) {
	clearStdout()
	fmt.Println("Ranking of Special Contest", contestName, "at", time.Now().Format("2006-01-02 15:04:05"))
	format := "%-4s %-20s %-5s %-12s %-9s %-7s %-9v %-9s %-7s %-9v %-9s %-7s %-9v %-9s %-7s %-9v\n"
	fmt.Printf(format, "Rank", "Name", "Score", "Finish Time",
		"Q1 (1)", "(sec)", "(WA)", "Q2 (2)", "(sec)", "(WA)", "Q3 (4)", "(sec)", "(WA)", "Q4 (8)", "(sec)", "(WA)")
	for i, v := range ts {
		ft1, ft2, ft3, ft4 := v.FinishTimes[0], v.FinishTimes[1], v.FinishTimes[2], v.FinishTimes[3]
		fmt.Printf(format, strconv.Itoa(i+1), trimSuffix(v.Name, 20), strconv.Itoa(v.Point), hms(v.TotalDuration),
			hms(ft1.Abs), count(ft1.Delta), count(ft1.Penalty), hms(ft2.Abs), count(ft2.Delta), count(ft2.Penalty),
			hms(ft3.Abs), count(ft3.Delta), count(ft3.Penalty), hms(ft4.Abs), count(ft4.Delta), count(ft4.Penalty))
	}
	fmt.Println()
}

func clearStdout() {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()
}
