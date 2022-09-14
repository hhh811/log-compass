package collect

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/hhh811/log-compass/log"
	"github.com/hhh811/log-compass/msg"
	"github.com/hhh811/log-compass/redis"
)

func TestIsIncrContent(t *testing.T) {
	t1 := "incr:key1:2"
	if r := isIncrContent(t1); !r {
		t.Errorf("%q isIncrContent %v expected %v", t1, r, true)
	}

	t1 = "idcr:key1:2"
	if r := isIncrContent(t1); r {
		t.Errorf("%q isIncrContent %v expected %v", t1, r, false)
	}

	t1 = "incr:key1:a2"
	if r := isIncrContent(t1); r {
		t.Errorf("%q isIncrContent %v expected %v", t1, r, false)
	}
}

func TestIncrKey(t *testing.T) {
	log.Setup(log.LogConf{
		Level: "debug",
	})

	c := redis.GetConn()
	ctx := context.Background()
	rc := NewRedisConsumer(ctx, c)

	key := "test_redis"
	expected := 0
	for i := 0; i < 5; i++ {
		v := 1
		expected += 1
		if err := rc.incrKey(key, int64(v)); err != nil {
			t.Errorf("incr key error: %v", err)
		}
	}

	got, err := c.Get(ctx, key).Result()
	if err != nil {
		t.Errorf("get key err: %v", err)
	}

	sum, err := strconv.ParseInt(got, 10, 64)
	if err != nil {
		t.Errorf("got not a number: %s", got)
	}

	if sum != int64(expected) {
		t.Errorf("expected %d got %d", expected, sum)
	}

	if _, err = c.Del(ctx, key).Result(); err != nil {
		t.Errorf("delete key error: %v", err)
	}
}

func TestConsume(t *testing.T) {
	log.Setup(log.LogConf{
		Level: "debug",
	})

	c := redis.GetConn()
	ctx := context.Background()
	rc := NewRedisConsumer(ctx, c)

	key := "test_redis"
	expected := 0
	msgs := []msg.Message{}
	for i := 0; i < 5; i++ {
		v := 1
		expected += 1
		msgs := append(msgs, makeIncrMsg(v, key))
		if err := rc.Consume(msgs); err != nil {
			t.Errorf("incr key error: %v", err)
		}
	}

	got, err := c.Get(ctx, key).Result()
	if err != nil {
		t.Errorf("get key err: %v", err)
	}

	sum, err := strconv.ParseInt(got, 10, 64)
	if err != nil {
		t.Errorf("got not a number: %s", got)
	}

	if sum != int64(expected) {
		t.Errorf("expected %d got %d", expected, sum)
	}

	if _, err = c.Del(ctx, key).Result(); err != nil {
		t.Errorf("delete key error: %v", err)
	}
}

func TestRedisCollect(t *testing.T) {
	log.Setup(log.LogConf{
		Level: "info",
	})

	ctx := context.Background()
	c := redis.GetConn()
	rc := NewRedisConsumer(ctx, c)

	cf := CollectorConf{
		triggerCacheSize:    0,
		triggerMilliSeconds: 10,
	}

	co := NewCollector(cf, rc)

	co.Start()

	expected := 0
	key := "test_redis"
	for i := 0; i < 100000; i++ {
		v := rand.Intn(100)
		// v := 1
		expected += v
		ms := makeIncrMsg(v, key)
		co.Collect(ms)
		log.Debugf("sent %d", v)
	}

	// time.Sleep(1 * time.Second)

	co.Stop()

	got, err := c.Get(ctx, key).Result()
	if err != nil {
		t.Errorf("get key err: %v", err)
	}

	sum, err := strconv.ParseInt(got, 10, 64)
	if err != nil {
		t.Errorf("got not a number: %s", got)
	}

	if _, err = c.Del(ctx, key).Result(); err != nil {
		t.Errorf("delete key error: %v", err)
	}

	if sum != int64(expected) {
		t.Errorf("expected %d got %d", expected, sum)
	}
}

func makeIncrMsg(v int, key string) msg.Message {
	return msg.Message{
		CollectTime: time.Now(),
		Label:       "redis_test",
		Creator:     "h",
		Content:     fmt.Sprintf("incr:%s:%d", key, v),
	}
}
