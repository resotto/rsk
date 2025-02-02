package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/redis/go-redis/v9"
)

var (
	CreateTableUserLogins = `
		CREATE TABLE IF NOT EXISTS UserLogins (
			UserId TEXT PRIMARY KEY,
			LastLogin TEXT
		)
	`
	InsertUserLoginsUserIdLastLogin = `
		INSERT INTO UserLogins 
		(UserId, LastLogin)
		VALUES (?, ?)	
	`
	UpdateUserLoginsLastLoginByUserId = `
		UPDATE UserLogins 
		SET LastLogin = ?
		WHERE UserId = ?	
	`
	SelectUserLoginsLastLoginByUserId = `
		SELECT LastLogin
		FROM UserLogins
		WHERE UserId = ?	
	`
	cluster                         bool
	instances, numreplicas, timeout int
	cli                             redis.UniversalClient
)

func parseFlags() {
	flag.BoolVar(&cluster, "cluster", false, "")
	flag.IntVar(&instances, "instances", 1, "")
	flag.IntVar(&numreplicas, "numreplicas", 1, "")
	flag.IntVar(&timeout, "timeout", 4000, "")

	flag.Parse()
}

func main() {
	parseFlags()

	if cluster {
		addrs := make([]string, 0)
		for i := 0; i < instances; i++ {
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

	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"

	session, err := cluster.CreateSession()
	if err != nil {
		panic("failed to connect to cluster")
	}
	defer session.Close()

	if err := session.Query(CreateTableUserLogins).Exec(); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	UserID := "user_id_1"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	execute(ctx, session, UserID, false)

	fmt.Printf("\n************ ************ ************ ************ ************ ************ ************ ************\n\n")

	execute(ctx, session, UserID, true)
}

func execute(ctx context.Context, session *gocql.Session, UserId string, invalidate bool) {
	if _, err := cacheAsideWrite(ctx, session, UserId, invalidate, false, insert); err != nil {
		log.Fatalf("initial cache-aside write failed: %v", err)
	}
	fmt.Printf("****** 1st cache-aside write ends ******\n\n")

	writeResult, err := cacheAsideWrite(ctx, session, UserId, invalidate, true, update)
	if err != nil {
		fmt.Printf("cache-aside write failed intentionally: %v\n", err)
	}
	fmt.Printf("****** 2nd cache-aside write ends ******\n\n")

	readResult, err := cacheAsideRead(ctx, session, UserId)
	if err != nil {
		log.Fatalf("cache-aside read failed: %v", err)
	}
	fmt.Printf("******    cache-aside read ends   ******\n\n")

	if writeResult == readResult {
		fmt.Println("write result and next read result are synchronized [OK]")
	} else {
		fmt.Println("write result and next read result are inconsistent [NG]")
	}
}

func cacheAsideRead(ctx context.Context, session *gocql.Session, userID string) (string, error) {
	res, err := cli.Get(ctx, userID).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			// omit retry logic for convenience
			return "", fmt.Errorf("failed to get data from cache: %v", err)
		}
	} else if res != "" {
		fmt.Println("retrieving data from cache succeeded:", res)
		return res, nil
	}
	if err = session.Query(SelectUserLoginsLastLoginByUserId, userID).Scan(&res); err != nil {
		// omit retry logic for convenience
		return "", fmt.Errorf("failed to query database: %v", err)
	}
	return updateCache(ctx, userID, res, "database query succeeded:")
}

func cacheAsideWrite(ctx context.Context, session *gocql.Session, userID string, invalidate, causeError bool,
	f func(session *gocql.Session, userID, t string) (string, error)) (string, error) {
	if invalidate {
		if _, err := cli.Del(ctx, userID).Result(); err != nil {
			return "", err
		}
		fmt.Println("cache invalidation succeeded")
	}
	t := time.Now().Format("15:04:05.00000")
	if _, err := f(session, userID, t); err != nil {
		return "", err
	}
	if causeError {
		return t, fmt.Errorf("error caused before updating cache with: %v", t)
	}
	return updateCache(ctx, userID, t, "cache update succeeded with:")
}

func updateCache(ctx context.Context, userID, val, printMessage string) (string, error) {
	if _, err := cli.Set(ctx, userID, val, 60*time.Minute).Result(); err != nil {
		// omit retry logic for convenience
		log.Fatalf("Failed to update data into cache: %v", err)
	}
	if cluster {
		if _, err := waitof(ctx); err != nil {
			return "", err
		}
	}
	fmt.Println(printMessage, val)
	return val, nil
}

func insert(session *gocql.Session, userID, t string) (string, error) {
	var res string
	if err := session.Query(SelectUserLoginsLastLoginByUserId, userID).Scan(&res); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			if err = session.Query(InsertUserLoginsUserIdLastLogin, userID, t).Exec(); err != nil {
				return "", fmt.Errorf("failed to insert data into database: %v", err)
			}
		} else {
			// omit retry logic for convenience
			return "", fmt.Errorf("failed to query database: %v", err)
		}
	}
	fmt.Println("database insert succeeded with:", t)
	return res, nil
}

func update(session *gocql.Session, userID, t string) (string, error) {
	var res string
	if err := session.Query(SelectUserLoginsLastLoginByUserId, userID).Scan(&res); err != nil {
		// omit retry logic for convenience
		return "", fmt.Errorf("failed to query database: %v", err)
	} else if err := session.Query(UpdateUserLoginsLastLoginByUserId, t, userID).Exec(); err != nil {
		// omit retry logic for convenience
		return "", fmt.Errorf("failed to update database: %v", err)
	}
	fmt.Println("database update succeeded with:", t)
	return res, nil
}

func waitof(ctx context.Context) (string, error) {
	res, err := cli.Do(ctx, "WAIT", numreplicas, timeout).Result()
	if err != nil {
		return "", err
	}
	resVal, ok := res.(int64)
	if !ok {
		return "", fmt.Errorf("unexpected type for res: %T", res)
	} else if resVal < int64(numreplicas) {
		return "", fmt.Errorf("replica acknowledgement failed: got %v but expected %v", resVal, numreplicas)
	}
	return "", nil
}
