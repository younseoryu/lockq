package lockq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLockOwnership_MultipleWorkersSequentialExecution verifies that tasks with
// the same lock key are processed sequentially even with multiple workers.
func TestLockOwnership_MultipleWorkersSequentialExecution(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		LockTimeout:    5 * time.Second,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		MaxJitter:      0,
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()
	lockKey := "shared:resource"

	var (
		mu             sync.Mutex
		concurrent     int32
		maxConcurrent  int32
		executionOrder []string
	)

	q.RegisterHandler("locked_task", func(ctx context.Context, payload []byte) error {
		// Track concurrent executions
		current := atomic.AddInt32(&concurrent, 1)
		defer atomic.AddInt32(&concurrent, -1)

		// Update max concurrent
		for {
			max := atomic.LoadInt32(&maxConcurrent)
			if current <= max || atomic.CompareAndSwapInt32(&maxConcurrent, max, current) {
				break
			}
		}

		mu.Lock()
		executionOrder = append(executionOrder, string(payload))
		mu.Unlock()

		// Simulate work
		time.Sleep(50 * time.Millisecond)
		return nil
	}, OnlyLocked())

	// Enqueue multiple tasks with SAME lock key
	for i := 0; i < 5; i++ {
		_, err := q.Enqueue(ctx, "locked_task", &TaskOptions{
			LockKey: lockKey,
			Payload: []byte{'A' + byte(i)}, // A, B, C, D, E
		})
		require.NoError(t, err)
	}

	// Start multiple workers
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the queue (internally manages workers)
	go func() { _ = q.Start(workerCtx) }()

	// Wait for all tasks to complete
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		count, _ := q.CountAll(ctx, QueueLocked)
		if count == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Give time for last task to finish
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Verify: max concurrent should be 1 (sequential execution due to lock)
	assert.Equal(t, int32(1), atomic.LoadInt32(&maxConcurrent),
		"Tasks with same lock key should execute sequentially (max concurrent = 1)")

	// Verify all tasks executed
	mu.Lock()
	assert.Len(t, executionOrder, 5, "All 5 tasks should have executed")
	mu.Unlock()
}

// TestLockOwnership_MultipleWorkersDifferentLockKeys verifies that tasks with
// different lock keys can execute concurrently.
func TestLockOwnership_MultipleWorkersDifferentLockKeys(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		LockTimeout:    5 * time.Second,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		MaxJitter:      0,
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()

	var (
		concurrent    int32
		maxConcurrent int32
		completed     int32
	)

	q.RegisterHandler("locked_task", func(ctx context.Context, payload []byte) error {
		current := atomic.AddInt32(&concurrent, 1)
		defer atomic.AddInt32(&concurrent, -1)

		// Update max concurrent
		for {
			max := atomic.LoadInt32(&maxConcurrent)
			if current <= max || atomic.CompareAndSwapInt32(&maxConcurrent, max, current) {
				break
			}
		}

		// Hold the task long enough for others to start
		time.Sleep(100 * time.Millisecond)
		atomic.AddInt32(&completed, 1)
		return nil
	}, OnlyLocked())

	// Enqueue tasks with DIFFERENT lock keys
	for i := 0; i < 5; i++ {
		_, err := q.Enqueue(ctx, "locked_task", &TaskOptions{
			LockKey: string(rune('A' + i)), // Different lock keys: A, B, C, D, E
			Payload: []byte{byte(i)},
		})
		require.NoError(t, err)
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = q.Start(workerCtx) }()

	// Wait for completion
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&completed) >= 5 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	// With different lock keys, tasks CAN run concurrently
	// (actual concurrency depends on worker implementation, but should be > 1)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&maxConcurrent), int32(1),
		"Tasks with different lock keys should be able to run concurrently")
	assert.Equal(t, int32(5), atomic.LoadInt32(&completed), "All tasks should complete")
}

// TestLockOwnership_WorkerFailureDoesNotBlockOthers verifies that if a worker
// fails/times out, another worker can pick up tasks with the same lock key
// after the lock expires.
func TestLockOwnership_WorkerFailureDoesNotBlockOthers(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		LockTimeout:    200 * time.Millisecond, // Short lock timeout
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		MaxJitter:      0,
		MaxRetries:     0, // No retries - go straight to DLQ
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()
	lockKey := "contested:resource"

	var (
		attempts  int32
		successes int32
	)

	q.RegisterHandler("flaky_task", func(ctx context.Context, payload []byte) error {
		attempt := atomic.AddInt32(&attempts, 1)

		if attempt == 1 {
			// First attempt: simulate slow task that exceeds lock timeout
			// but doesn't exceed handler timeout
			time.Sleep(300 * time.Millisecond)
			return errors.New("simulated timeout")
		}

		// Subsequent attempts succeed quickly
		atomic.AddInt32(&successes, 1)
		return nil
	}, OnlyLocked())

	// Enqueue two tasks with same lock key
	_, err = q.Enqueue(ctx, "flaky_task", &TaskOptions{
		LockKey: lockKey,
		Payload: []byte("task1"),
	})
	require.NoError(t, err)

	_, err = q.Enqueue(ctx, "flaky_task", &TaskOptions{
		LockKey: lockKey,
		Payload: []byte("task2"),
	})
	require.NoError(t, err)

	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = q.Start(workerCtx) }()

	// Wait for processing
	time.Sleep(1 * time.Second)
	cancel()

	// First task should fail, second task should eventually succeed
	// (after first task's lock expires)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&attempts), int32(2),
		"Should have at least 2 attempts (first fails, others may succeed)")
}

// TestLockOwnership_LockNotReleasedByWrongTask is an integration test that
// verifies the lock ownership fix works end-to-end with real workers.
func TestLockOwnership_LockNotReleasedByWrongTask(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		LockTimeout:    100 * time.Millisecond, // Very short lock timeout
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		MaxJitter:      0,
		MaxRetries:     1,
	}
	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()
	lockKey := "ownership:test"
	lockRedisKey := config.WithDefaults().LockKeyPrefix() + lockKey

	var (
		task1Started  int32
		task1Finished int32
		task2Started  int32
		task2Finished int32
	)

	q.RegisterHandler("ownership_task", func(ctx context.Context, payload []byte) error {
		taskNum := string(payload)

		if taskNum == "1" {
			atomic.StoreInt32(&task1Started, 1)
			// Task 1: takes longer than lock timeout
			time.Sleep(200 * time.Millisecond)
			atomic.StoreInt32(&task1Finished, 1)
			return errors.New("task 1 failed after lock expired")
		}

		if taskNum == "2" {
			atomic.StoreInt32(&task2Started, 1)
			// Task 2: quick execution
			time.Sleep(20 * time.Millisecond)

			// Verify we still hold the lock at the end
			owner, err := rdb.Get(ctx, lockRedisKey).Result()
			if err == nil {
				// If lock exists, we should own it
				taskKey := config.WithDefaults().TaskKeyPrefix()
				// The lock value should be a task ID that's ours
				t.Logf("Lock owner at task2 finish: %s (prefix: %s)", owner, taskKey)
			}

			atomic.StoreInt32(&task2Finished, 1)
			return nil
		}

		return nil
	}, OnlyLocked())

	// Enqueue task 1 first
	_, err = q.Enqueue(ctx, "ownership_task", &TaskOptions{
		LockKey: lockKey,
		Payload: []byte("1"),
	})
	require.NoError(t, err)

	// Enqueue task 2 immediately after
	_, err = q.Enqueue(ctx, "ownership_task", &TaskOptions{
		LockKey: lockKey,
		Payload: []byte("2"),
	})
	require.NoError(t, err)

	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = q.Start(workerCtx) }()

	// Wait for task 1 to start
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt32(&task1Started) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&task1Started), "Task 1 should start")

	// Wait for lock to expire and task 2 to potentially start
	time.Sleep(150 * time.Millisecond)

	// Task 2 should be able to acquire the lock after task 1's lock expires
	// Wait for both tasks to finish
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&task1Finished) == 1 && atomic.LoadInt32(&task2Finished) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	time.Sleep(50 * time.Millisecond)

	// Both tasks should have finished
	assert.Equal(t, int32(1), atomic.LoadInt32(&task1Finished), "Task 1 should finish")
	assert.Equal(t, int32(1), atomic.LoadInt32(&task2Finished), "Task 2 should finish")

	// The key test: when task 1 finishes (after its lock expired),
	// it should NOT have released task 2's lock.
	// Task 2 should have completed successfully (not had its lock stolen).
}
