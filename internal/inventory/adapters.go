package inventory

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache struct that proxies to Redis
type RedisCache struct {
	client *redis.Client
}

// NewRedisCache function creates a proxy to Redis that satisfies the Cache interface
func NewRedisCache(client *redis.Client) *RedisCache {
	return &RedisCache{client: client}
}

func (r *RedisCache) Get(ctx context.Context, key string) (string, error) {
	val, err := r.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

func (r *RedisCache) Set(ctx context.Context, key, value string, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}
