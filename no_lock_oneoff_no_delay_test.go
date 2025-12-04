package lockq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNoLockOneOffNoDelay tests a one-off task without lock key and no delay
func TestNoLockOneOffNoDelay(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()

	var executed bool
	var receivedPayload []byte
	var mu sync.Mutex

	// Register handler
	q.RegisterHandler("test_task", func(ctx context.Context, payload []byte) error {
		mu.Lock()
		executed = true
		receivedPayload = payload
		mu.Unlock()
		return nil
	}, OnlyUnlocked())

	// Enqueue task with no delay
	payload := []byte("test-payload")
	taskID, err := q.Enqueue(ctx, "test_task", &TaskOptions{
		Payload: payload,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, taskID)

	// DETERMINISTIC: Verify task is ready immediately
	count, err := q.Count(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "Task should be ready immediately")

	// DETERMINISTIC: Verify task exists in total count
	totalCount, err := q.CountAll(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(1), totalCount, "Task should exist in queue")

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for execution (check actual execution, not timing)
	deadline := time.Now().Add(1 * time.Second)
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
	mu.Unlock()

	// DETERMINISTIC: Verify task is removed from queue
	finalCount, err := q.CountAll(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(0), finalCount, "Task should be removed after execution")
}
