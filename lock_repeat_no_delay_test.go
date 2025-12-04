package lockq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLockRepeatNoDelay tests a repeating task with lock key and no initial delay
func TestLockRepeatNoDelay(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()
	lockKey := "user:123"

	var execCount int32
	var mu sync.Mutex
	executionTimes := make([]time.Time, 0)

	// Register handler
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		mu.Lock()
		executionTimes = append(executionTimes, time.Now())
		mu.Unlock()
		atomic.AddInt32(&execCount, 1)
		return nil
	}, OnlyLocked())

	// Enqueue repeat task with no initial delay and 200ms interval
	interval := 200 * time.Millisecond
	payload := []byte("repeat-task")
	taskID, err := q.EnqueueRepeat(ctx, "test_task", &TaskOptions{
		LockKey: lockKey,
		Payload: payload,
	}, interval)
	require.NoError(t, err)
	assert.NotEmpty(t, taskID)

	// DETERMINISTIC: Verify task is ready immediately
	count, err := q.Count(ctx, QueueLocked)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "Task should be ready immediately")

	// DETERMINISTIC: Verify it's in queue
	totalCount, err := q.CountAll(ctx, QueueLocked)
	require.NoError(t, err)
	assert.Equal(t, int64(1), totalCount, "Task should exist in queue")

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for at least 2 executions
	deadline := time.Now().Add(interval*2 + 500*time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&execCount) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// DETERMINISTIC: Verify task executed at least twice
	finalCount := atomic.LoadInt32(&execCount)
	assert.GreaterOrEqual(t, finalCount, int32(2), "Task should execute at least twice")

	// DETERMINISTIC: Verify task is still in queue (repeat tasks persist)
	totalCount, err = q.CountAll(ctx, QueueLocked)
	require.NoError(t, err)
	assert.Equal(t, int64(1), totalCount, "Repeat task should remain in queue")

	// DETERMINISTIC: Verify executions are spaced (with reasonable tolerance)
	mu.Lock()
	if len(executionTimes) >= 2 {
		actualInterval := executionTimes[1].Sub(executionTimes[0])
		// Allow wide tolerance for test flakiness
		assert.GreaterOrEqual(t, actualInterval, interval-100*time.Millisecond, "Executions should be roughly spaced by interval")
		assert.LessOrEqual(t, actualInterval, interval+200*time.Millisecond, "Executions should be roughly spaced by interval")
	}
	mu.Unlock()
}
