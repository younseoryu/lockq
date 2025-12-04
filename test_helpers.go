package lockq

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

// setupTestRedis creates a Redis client for testing.
func setupTestRedis(t *testing.T) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // Use a separate DB for tests
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	require.NoError(t, rdb.FlushDB(ctx).Err())

	t.Cleanup(func() {
		_ = rdb.FlushDB(context.Background())
		_ = rdb.Close()
	})

	return rdb
}

// setupTestQueue creates a Queue with fast polling for tests.
func setupTestQueue(t *testing.T, rdb *redis.Client) *Queue {
	config := &Config{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     1 * time.Millisecond,
		MaxJitter:      0,
		ErrorDelay:     10 * time.Millisecond,
	}

	q, err := New(rdb, config)
	require.NoError(t, err)
	return q
}
