package collect

import (
	"context"
	"strconv"
	"strings"

	"regexp"

	"github.com/go-redis/redis/v8"
	"github.com/hhh811/log-compass/log"
	"github.com/hhh811/log-compass/msg"
)

var incrContent = regexp.MustCompile(`^incr:[a-z|A-Z|0-9|-|_]+:\d+$`)

type RedisConsumer struct {
	ctx    context.Context
	client *redis.Client
}

func NewRedisConsumer(ctx context.Context, client *redis.Client) *RedisConsumer {
	return &RedisConsumer{
		ctx:    ctx,
		client: client,
	}
}

func (r *RedisConsumer) Consume(msgs []msg.Message) error {
	for _, m := range msgs {
		r.ConsumeSingle(m)
	}
	return nil
}

func (r *RedisConsumer) ConsumeSingle(ms msg.Message) error {
	if isIncrContent(ms.Content) {
		split := strings.Split(ms.Content, ":")
		key := split[1]
		v, _ := strconv.ParseInt(split[2], 10, 64)
		return r.incrKey(key, v)
	}
	return nil
}

func (r *RedisConsumer) incrKey(key string, v int64) error {
	rs, err := r.client.IncrBy(r.ctx, key, v).Result()
	log.Debugf("incr %s by %d, rs %d", key, v, rs)
	if err != nil {
		log.Errorf("incr key %s by %d err: %v", key, v, err)
	}
	// todo wrap errors
	return err
}

func isIncrContent(content string) bool {
	return incrContent.MatchString(content)
}
