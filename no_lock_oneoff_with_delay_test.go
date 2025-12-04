package lockq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNoLockOneOffWithDelay tests a one-off task without lock key that has an initial delay
func TestNoLockOneOffWithDelay(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()

	var executed bool
	var receivedPayload []byte
	var executionTime time.Time
	var mu sync.Mutex

	// Register handler
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		mu.Lock()
		executed = true
		receivedPayload = payload
		executionTime = time.Now()
		mu.Unlock()
		return nil
	}, OnlyUnlocked())

	// Enqueue task with 200ms delay
	delay := 200 * time.Millisecond
	payload := []byte("test-payload")

	enqueueTime := time.Now()
	taskID, err := q.Enqueue(ctx, "test_task", &TaskOptions{
		Payload: payload,
		Delay:   delay,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, taskID)

	// DETERMINISTIC: Verify task is NOT ready immediately
	count, err := q.Count(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "Task should not be ready immediately")

	// DETERMINISTIC: Verify task exists in total count
	totalCount, err := q.CountAll(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(1), totalCount, "Task should exist in queue")

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for execution
	deadline := time.Now().Add(delay + 500*time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := executed
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// DETERMINISTIC: Verify task was executed
	mu.Lock()
	assert.True(t, executed, "Task should be executed")
	assert.Equal(t, payload, receivedPayload, "Payload should match")

	// DETERMINISTIC: Verify execution happened after delay (with tolerance)
	actualDelay := executionTime.Sub(enqueueTime)
	assert.GreaterOrEqual(t, actualDelay, delay-50*time.Millisecond, "Task should execute after delay")
	mu.Unlock()

	// DETERMINISTIC: Verify task is removed from queue
	finalCount, err := q.CountAll(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(0), finalCount, "Task should be removed after execution")
}
