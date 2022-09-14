package redis

import "github.com/go-redis/redis/v8"

// todo pool
func GetConn() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	return rdb
}
