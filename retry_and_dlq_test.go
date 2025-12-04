package lockq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOneOffTask_SuccessfulRetry tests that a one-off task retries on failure and eventually succeeds.
func TestOneOffTask_SuccessfulRetry(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		MaxRetries: 3, // 1 initial + 3 retries = 4 total attempts
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()
	var attempts int32

	// Handler that fails first 2 times, succeeds on 3rd
	q.RegisterHandler("oneoff_task", func(ctx context.Context, payload []byte) error {
		current := atomic.AddInt32(&attempts, 1)
		if current < 3 {
			return errors.New("intentional failure")
		}
		return nil
	}, OnlyUnlocked())

	// Enqueue one-off task
	taskID, err := q.Enqueue(ctx, "oneoff_task", &TaskOptions{
		Payload: []byte("test"),
	})
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for retries to complete
	time.Sleep(10 * time.Second)

	// Verify task attempted 3 times and succeeded
	finalAttempts := atomic.LoadInt32(&attempts)
	assert.GreaterOrEqual(t, finalAttempts, int32(3), "Task should retry until success")

	// Verify task not in DLQ (it succeeded)
	dlqCount, err := q.GetDeadLetterQueueCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), dlqCount, "Successful task should not be in DLQ")

	// Verify task removed from queue (one-off completed)
	count, err := q.CountAll(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "One-off task should be removed after success")

	_ = q.DeleteTask(ctx, taskID)
}

// TestOneOffTask_ExhaustRetries tests that a one-off task goes to DLQ after exhausting retries.
func TestOneOffTask_ExhaustRetries(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		MaxRetries: 2, // 1 initial + 2 retries = 3 total attempts
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()
	var attempts int32

	// Handler that always fails
	q.RegisterHandler("failing_task", func(ctx context.Context, payload []byte) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("permanent failure")
	}, OnlyUnlocked())

	// Enqueue task
	taskID, err := q.Enqueue(ctx, "failing_task", &TaskOptions{
		Payload: []byte("test"),
	})
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for retries to exhaust
	time.Sleep(10 * time.Second)

	// Verify task attempted MaxAttempts times
	finalAttempts := atomic.LoadInt32(&attempts)
	assert.Equal(t, int32(3), finalAttempts, "Task should be attempted MaxRetries+1 times")

	// Verify task in DLQ
	dlqCount, err := q.GetDeadLetterQueueCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlqCount, "Failed task should be in DLQ")

	// Verify task removed from active queue
	count, err := q.CountAll(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "Failed task should be removed from active queue")

	_ = q.DeleteTask(ctx, taskID)
}

// TestRepeatTask_SuccessfulExecution tests that repeating tasks reset attempts counter on success.
func TestRepeatTask_SuccessfulExecution(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		MaxRetries: 3, // MaxAttempts = 4
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()
	var execCount int32

	// Handler that always succeeds
	q.RegisterHandler("repeat_success", func(ctx context.Context, payload []byte) error {
		atomic.AddInt32(&execCount, 1)
		return nil
	}, OnlyUnlocked())

	// Enqueue repeating task
	taskID, err := q.EnqueueRepeat(ctx, "repeat_success", &TaskOptions{
		Payload: []byte("test"),
	}, 100*time.Millisecond)
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Let it run for multiple intervals.
	// Use a generous duration so this remains stable even under -race,
	// which significantly slows execution.
	time.Sleep(1 * time.Second)

	// Verify task executed multiple times (more than MaxAttempts)
	finalCount := atomic.LoadInt32(&execCount)
	assert.Greater(t, finalCount, int32(4), "Repeating task should execute beyond MaxAttempts")

	// Wait for one more execution to complete (so attempts get reset)
	time.Sleep(200 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Check attempts counter in Redis (should be 0 or 1 - may have just been incremented)
	taskKey := q.config.TaskKeyPrefix() + taskID
	attemptsStr, err := rdb.HGet(ctx, taskKey, "at").Result()
	require.NoError(t, err)
	// After success, attempts should be 0, but might be 1 if just popped
	assert.Contains(t, []string{"0", "1"}, attemptsStr, "Attempts should be low after successful executions")

	// Verify task still in queue (repeating tasks persist)
	count, err := q.CountAll(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "Repeating task should remain in queue")

	// Verify not in DLQ
	dlqCount, err := q.GetDeadLetterQueueCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), dlqCount, "Successful repeating task should not be in DLQ")

	_ = q.DeleteTask(ctx, taskID)
}

// TestRepeatTask_FlakyRecovery tests that a repeating task recovers from failures.
func TestRepeatTask_FlakyRecovery(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		MaxRetries: 2, // MaxAttempts = 3
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()
	var execCount int32

	// Handler that fails first 2 times, then succeeds
	q.RegisterHandler("flaky_task", func(ctx context.Context, payload []byte) error {
		count := atomic.AddInt32(&execCount, 1)
		if count <= 2 {
			return errors.New("temporary failure")
		}
		return nil
	}, OnlyUnlocked())

	// Enqueue repeating task
	taskID, err := q.EnqueueRepeat(ctx, "flaky_task", &TaskOptions{
		Payload: []byte("test"),
	}, 200*time.Millisecond)
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for failures, retries, and recovery
	time.Sleep(10 * time.Second)

	finalCount := atomic.LoadInt32(&execCount)
	t.Logf("Task executed %d times", finalCount)

	// Task should recover and continue repeating
	assert.GreaterOrEqual(t, finalCount, int32(3), "Task should retry and recover")

	// Check attempts counter reset after success (should be 0 or low)
	taskKey := q.config.TaskKeyPrefix() + taskID
	attemptsStr, err := rdb.HGet(ctx, taskKey, "at").Result()
	require.NoError(t, err)
	// After recovery with successes, attempts should be reset
	assert.Contains(t, []string{"0", "1"}, attemptsStr, "Attempts should reset after successful recovery")

	// Should still be in queue, not DLQ
	count, err := q.CountAll(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "Task should remain in queue after recovery")

	dlqCount, err := q.GetDeadLetterQueueCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), dlqCount, "Recovered task should not be in DLQ")

	_ = q.DeleteTask(ctx, taskID)
}

// TestRepeatTask_PermanentFailure tests that a repeating task goes to DLQ on permanent failure.
func TestRepeatTask_PermanentFailure(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		MaxRetries: 2, // MaxAttempts = 3
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()
	var attempts int32

	// Handler that always fails
	q.RegisterHandler("repeat_failing", func(ctx context.Context, payload []byte) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("permanent failure")
	}, OnlyUnlocked())

	// Enqueue repeating task
	taskID, err := q.EnqueueRepeat(ctx, "repeat_failing", &TaskOptions{
		Payload: []byte("test"),
	}, 100*time.Millisecond)
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for retries to exhaust
	time.Sleep(10 * time.Second)

	// Should have attempted MaxAttempts times before going to DLQ
	finalAttempts := atomic.LoadInt32(&attempts)
	assert.GreaterOrEqual(t, finalAttempts, int32(3), "Should attempt at least MaxRetries+1 times before DLQ")

	// Should be in DLQ now
	dlqCount, err := q.GetDeadLetterQueueCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlqCount, "Permanently failing repeat task should go to DLQ")

	// Should be removed from active queue
	count, err := q.CountAll(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "Failed task should be removed from active queue")

	_ = q.DeleteTask(ctx, taskID)
}

// TestDLQ_TaskPersistence tests that tasks in DLQ don't expire.
func TestDLQ_TaskPersistence(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		LockTimeout: 1 * time.Second,
		MaxRetries:  1, // Only 2 attempts total
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()

	// Handler that always fails
	q.RegisterHandler("dlq_test", func(ctx context.Context, payload []byte) error {
		return errors.New("failure")
	}, OnlyUnlocked())

	// Enqueue task
	taskID, err := q.Enqueue(ctx, "dlq_test", &TaskOptions{
		Payload: []byte("test"),
	})
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for task to fail and move to DLQ
	time.Sleep(5 * time.Second)

	// Verify in DLQ
	dlqCount, err := q.GetDeadLetterQueueCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlqCount, "Failed task should be in DLQ")

	// Wait longer than LockTimeout to test if task expires
	time.Sleep(2 * time.Second)

	// Task data should still exist (PERSIST was called)
	taskKey := q.config.TaskKeyPrefix() + taskID
	exists, err := rdb.Exists(ctx, taskKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "Task data should persist in DLQ")

	// Check TTL - should be -1 (no expiration) or -2 (key doesn't exist which is also ok)
	ttl, err := rdb.TTL(ctx, taskKey).Result()
	require.NoError(t, err)
	if exists == 1 {
		assert.Equal(t, time.Duration(-1), ttl, "Task in DLQ should have no expiration")
	}

	_ = q.DeleteTask(ctx, taskID)
}

// TestDLQ_RetryFromDLQ tests retrying a task from the dead letter queue.
func TestDLQ_RetryFromDLQ(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		MaxRetries: 1,
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()
	var attempts int32

	// Handler that fails first 3 times, then succeeds
	q.RegisterHandler("retry_from_dlq", func(ctx context.Context, payload []byte) error {
		count := atomic.AddInt32(&attempts, 1)
		if count <= 3 {
			return errors.New("failure")
		}
		return nil
	}, OnlyUnlocked())

	// Enqueue task
	taskID, err := q.Enqueue(ctx, "retry_from_dlq", &TaskOptions{
		Payload: []byte("test"),
	})
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for task to fail and go to DLQ
	time.Sleep(5 * time.Second)

	// Verify in DLQ
	dlqCount, err := q.GetDeadLetterQueueCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlqCount, "Failed task should be in DLQ")

	// Retry from DLQ
	err = q.RetryFromDLQ(ctx, taskID)
	require.NoError(t, err)

	// Wait for retry
	time.Sleep(5 * time.Second)

	// Task should eventually succeed and be removed
	finalAttempts := atomic.LoadInt32(&attempts)
	assert.GreaterOrEqual(t, finalAttempts, int32(4), "Task should retry after DLQ")

	// Should no longer be in DLQ
	dlqCount, err = q.GetDeadLetterQueueCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), dlqCount, "Successful retry should remove from DLQ")

	_ = q.DeleteTask(ctx, taskID)
}

// TestRepeatTask_NoHandler tests that repeating tasks without handlers are cleaned up.
func TestRepeatTask_NoHandler(t *testing.T) {
	rdb := setupTestRedis(t)
	q := setupTestQueue(t, rdb)

	ctx := context.Background()

	// DO NOT register a handler - testing missing handler scenario

	// Enqueue repeating task
	taskID, err := q.EnqueueRepeat(ctx, "no_handler_task", &TaskOptions{
		Payload: []byte("test"),
	}, 50*time.Millisecond)
	require.NoError(t, err)

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = q.Start(workerCtx) }()

	// Wait for task to be processed
	time.Sleep(200 * time.Millisecond)

	// Task should be deleted (not looping forever)
	count, err := q.CountAll(ctx, QueueUnlocked)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "Repeating task without handler should be removed")

	_ = q.DeleteTask(ctx, taskID)
}
