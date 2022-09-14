package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

func ExampleClient() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	err := rdb.Set(ctx, "key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := rdb.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := rdb.Get(ctx, "key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exist")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("key2", val2)
	}
}

func ExampleCornerCase() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	_, err := rdb.SetNX(ctx, "keyNx", "value", 10*time.Second).Result()
	if err != nil {
		fmt.Printf("set nex err: %+v\n", err)
	}

	_, err = rdb.SetNX(ctx, "keyTTL", "value", redis.KeepTTL).Result()
	if err != nil {
		fmt.Printf("set nex err: %+v\n", err)
	}

	vals1, err := rdb.Sort(ctx, "list", &redis.Sort{Offset: 0, Count: 2, Order: "ASC"}).Result()
	fmt.Printf("%+v\n", vals1)

	vals2, err := rdb.ZRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: 0,
		Count:  2,
	}).Result()
	fmt.Printf("%+v\n", vals2)

	vals3, err := rdb.ZInterStore(ctx, "out", &redis.ZStore{
		Keys:    []string{"zset1", "zset2"},
		Weights: []float64{2, 3},
	}).Result()
	fmt.Printf("%+v\n", vals3)

	vals4, err := rdb.Eval(ctx, "return {KEYS{1],ARGV[1]}", []string{"key"}, "hello").Result()
	fmt.Printf("%+v\n", vals4)

	res, err := rdb.Do(ctx, "set", "key", "value2").Result()
	fmt.Printf("%+v\n", res)
}

func ExampleR() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	if err := rdb.RPush(ctx, "queue", "message").Err(); err != nil {
		panic(err)
	}

	result, err := rdb.BLPop(ctx, 1*time.Second, "queue").Result()
	if err != nil {
		panic(err)
	}

	fmt.Println(result[0], result[1])
}

func ExampleIncr() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	result, err := rdb.Incr(ctx, "counter").Result()
	if err != nil {
		panic(err)
	}

	fmt.Println(result)
}

func ExampleScan() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	rdb.FlushDB(ctx)
	for i := 0; i < 33; i++ {
		err := rdb.Set(ctx, fmt.Sprintf("key%d", i), "value", 0).Err()
		if err != nil {
			panic(err)
		}
	}

	var cursor uint64
	var n int
	for {
		var keys []string
		var err error
		keys, cursor, err = rdb.Scan(ctx, cursor, "key*", 10).Result()
		if err != nil {
			panic(err)
		}
		n += len(keys)
		fmt.Println(keys)
		if cursor == 0 {
			break
		}
	}

	fmt.Printf("found %d keys\n", n)
}

func ExampleTrasaction1() {
	const maxRetries = 1000

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	increment := func(key string) error {
		txf := func(tx *redis.Tx) error {
			n, err := tx.Get(ctx, key).Int()
			if err != nil && err != redis.Nil {
				return err
			}

			n++

			_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Set(ctx, key, n, 0)
				return nil
			})
			return err
		}

		for i := 0; i < maxRetries; i++ {
			err := rdb.Watch(ctx, txf, key)
			if err == nil {
				return nil
			}
			if err == redis.TxFailedErr {
				continue
			}
			return err
		}

		return errors.New("increment reached maximum number of retries")
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := increment("counter3"); err != nil {
				fmt.Println("increment error: ", err)
			}
		}()
	}

	wg.Wait()

}

func ExamplePubSub1() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	pubsub := rdb.Subscribe(ctx, "mychannel1")

	_, err := pubsub.Receive(ctx)
	if err != nil {
		panic(err)
	}

	ch := pubsub.Channel()

	err = rdb.Publish(ctx, "mychannel1", "hello").Err()
	if err != nil {
		panic(err)
	}

	time.AfterFunc(time.Second, func() {
		_ = pubsub.Close()
	})

	for msg := range ch {
		fmt.Println(msg.Channel, msg.Payload)
	}
}

func ExamplePubSub2() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	pubsub := rdb.Subscribe(ctx, "mychannel2")

	defer pubsub.Close()

	for i := 0; i < 2; i++ {
		msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
		if err != nil {
			break
		}

		switch msg := msgi.(type) {
		case *redis.Subscription:
			fmt.Println("subscibe to", msg.Channel)

			_, err := rdb.Publish(ctx, "mychannel2", "hello").Result()
			if err != nil {
				panic(err)
			}
		case *redis.Message:
			fmt.Println("received", msg.Payload, " from", msg.Channel)
		default:
			panic("unreached")
		}
	}
}
