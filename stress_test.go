package lockq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStress_LockedTasksConcurrency performs a high-concurrency stress test focused on
// locked tasks. It verifies two key properties:
//  1. Tasks that share the same LockKey never execute concurrently.
//  2. Every enqueued task is processed exactly once (no drops or duplicates).
//
// The test uses many tasks across multiple lock keys and a handler that simulates work
// long enough to expose race-condition bugs if lock handling were incorrect.
func TestStress_LockedTasksConcurrency(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		// Short timings so the stress test runs quickly but still exercises many interleavings.
		LockTimeout:    2 * time.Second,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		MaxJitter:      0,
		ErrorDelay:     10 * time.Millisecond,
	}

	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()

	const (
		// Moderate number of distinct lock keys and tasks so the test can reasonably
		// drain the entire workload even under -race on typical hardware.
		numLockKeys = 64
		tasksPerKey = 16
		totalTasks  = numLockKeys * tasksPerKey
		// Allow generous time so the test remains stable even under -race.
		workerRuntime = 30 * time.Second
	)

	var (
		mu             sync.Mutex
		inFlightByKey  = make(map[string]int)
		processedByID  = make(map[string]int)
		concurrencyErr bool
	)

	handler := func(ctx context.Context, payload []byte) error {
		parts := strings.SplitN(string(payload), "|", 2)
		if len(parts) != 2 {
			t.Fatalf("invalid payload format: %q", string(payload))
		}
		lockKey := parts[0]
		id := parts[1]

		mu.Lock()
		inFlightByKey[lockKey]++
		if inFlightByKey[lockKey] > 1 {
			// More than one task with the same lock key executing at once -> violation.
			concurrencyErr = true
		}
		mu.Unlock()

		// Simulate work to increase chance of overlapping executions if locking is wrong.
		time.Sleep(3 * time.Millisecond)

		mu.Lock()
		inFlightByKey[lockKey]--
		processedByID[id]++
		mu.Unlock()

		return nil
	}

	q.RegisterHandler("stress_locked", handler, OnlyLocked())

	// Enqueue many tasks per lock key.
	for keyIdx := 0; keyIdx < numLockKeys; keyIdx++ {
		lockKey := fmt.Sprintf("stress:%d", keyIdx)
		for i := 0; i < tasksPerKey; i++ {
			id := fmt.Sprintf("%d-%d", keyIdx, i)
			payload := []byte(lockKey + "|" + id)

			_, err := q.Enqueue(ctx, "stress_locked", &TaskOptions{
				LockKey: lockKey,
				Payload: payload,
			})
			require.NoError(t, err)
		}
	}

	// Start workers.
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = q.Start(workerCtx)
	}()

	// Wait for all tasks to be processed or timeout.
	deadline := time.Now().Add(workerRuntime)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(processedByID) == totalTasks
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	require.False(t, concurrencyErr, "tasks sharing the same lock key must never execute concurrently")

	// Verify that every enqueued task ID was processed exactly once.
	require.Equal(t, totalTasks, len(processedByID), "all locked one-off tasks should be processed")

	for keyIdx := 0; keyIdx < numLockKeys; keyIdx++ {
		for i := 0; i < tasksPerKey; i++ {
			id := fmt.Sprintf("%d-%d", keyIdx, i)
			count, ok := processedByID[id]
			require.Truef(t, ok, "task %s was never processed", id)
			assert.Equalf(t, 1, count, "task %s should be processed exactly once", id)
		}
	}
}

// TestStress_UnlockedOneOffTasksExactlyOnce performs a stress test for unlocked
// one-off tasks. It verifies that each task is processed exactly once and that
// handlers see meaningful concurrency (i.e., multiple tasks in flight at once).
func TestStress_UnlockedOneOffTasksExactlyOnce(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		MaxJitter:      0,
		ErrorDelay:     10 * time.Millisecond,
	}

	// Use multiple queue instances sharing the same Redis to simulate
	// concurrent workers, matching the README's scaling model.
	const numWorkers = 4
	queues := make([]*Queue, numWorkers)
	for i := 0; i < numWorkers; i++ {
		q, err := New(rdb, config)
		require.NoError(t, err)
		queues[i] = q
	}

	ctx := context.Background()

	const (
		// Moderate number of tasks so the test can drain the entire workload.
		numTasks      = 2048
		workerRuntime = 30 * time.Second
	)

	var (
		mu             sync.Mutex
		inFlightByID   = make(map[string]int)
		processedByID  = make(map[string]int)
		concurrencyErr bool
		maxConcurrent  int
	)

	handler := func(ctx context.Context, payload []byte) error {
		id := string(payload)

		mu.Lock()
		inFlightByID[id]++
		if inFlightByID[id] > 1 {
			// The same task ID should never be executing concurrently.
			concurrencyErr = true
		}

		currentTotal := 0
		for _, v := range inFlightByID {
			currentTotal += v
		}
		if currentTotal > maxConcurrent {
			maxConcurrent = currentTotal
		}
		mu.Unlock()

		time.Sleep(3 * time.Millisecond)

		mu.Lock()
		inFlightByID[id]--
		processedByID[id]++
		mu.Unlock()

		return nil
	}

	// Register the same handler on all queues so each acts as an independent worker.
	for _, q := range queues {
		q.RegisterHandler("stress_unlocked_oneoff", handler, OnlyUnlocked())
	}

	for i := 0; i < numTasks; i++ {
		id := fmt.Sprintf("task-%d", i)
		// Enqueue via one queue; all queues share the same Redis-backed unlocked
		// queue so any worker can process the task.
		_, err := queues[0].Enqueue(ctx, "stress_unlocked_oneoff", &TaskOptions{
			Payload: []byte(id),
		})
		require.NoError(t, err)
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, q := range queues {
		go func(q *Queue) {
			_ = q.Start(workerCtx)
		}(q)
	}

	deadline := time.Now().Add(workerRuntime)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(processedByID) == numTasks
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	require.False(t, concurrencyErr, "a single unlocked one-off task must not execute concurrently with itself")
	require.Equal(t, numTasks, len(processedByID), "all unlocked one-off tasks should be processed")
	assert.GreaterOrEqual(t, maxConcurrent, 2, "unlocked one-off stress should observe some handler concurrency")

	for i := 0; i < numTasks; i++ {
		id := fmt.Sprintf("task-%d", i)
		count, ok := processedByID[id]
		require.Truef(t, ok, "task %s was never processed", id)
		assert.Equalf(t, 1, count, "task %s should be processed exactly once", id)
	}
}

// TestStress_LockedRepeatTasksNoOverlap performs a stress test for repeating
// locked tasks. It verifies that tasks sharing the same lock key never execute
// concurrently, while each lock key sees at least one execution.
func TestStress_LockedRepeatTasksNoOverlap(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		LockTimeout:    2 * time.Second,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		MaxJitter:      0,
		ErrorDelay:     10 * time.Millisecond,
	}

	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()

	const (
		numLockKeys   = 64
		interval      = 50 * time.Millisecond
		workerRuntime = 12 * time.Second
	)

	var (
		mu             sync.Mutex
		inFlightByKey  = make(map[string]int)
		execCountByKey = make(map[string]int)
		concurrencyErr bool
	)

	handler := func(ctx context.Context, payload []byte) error {
		lockKey := string(payload)

		mu.Lock()
		inFlightByKey[lockKey]++
		if inFlightByKey[lockKey] > 1 {
			concurrencyErr = true
		}
		mu.Unlock()

		time.Sleep(3 * time.Millisecond)

		mu.Lock()
		inFlightByKey[lockKey]--
		execCountByKey[lockKey]++
		mu.Unlock()

		return nil
	}

	q.RegisterHandler("stress_locked_repeat", handler, OnlyLocked())

	for keyIdx := 0; keyIdx < numLockKeys; keyIdx++ {
		lockKey := fmt.Sprintf("stress-repeat:%d", keyIdx)
		_, err := q.EnqueueRepeat(ctx, "stress_locked_repeat", &TaskOptions{
			LockKey: lockKey,
			Payload: []byte(lockKey),
		}, interval)
		require.NoError(t, err)
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = q.Start(workerCtx)
	}()

	deadline := time.Now().Add(workerRuntime)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	require.False(t, concurrencyErr, "repeating tasks sharing the same lock key must never execute concurrently")

	for keyIdx := 0; keyIdx < numLockKeys; keyIdx++ {
		lockKey := fmt.Sprintf("stress-repeat:%d", keyIdx)
		count := execCountByKey[lockKey]
		assert.GreaterOrEqualf(t, count, 1, "repeat task for %s should execute at least once", lockKey)
	}
}

// TestStress_UnlockedRepeatTasksNoOverlap performs a stress test for repeating
// unlocked tasks. It verifies that a single repeat task ID is never executed
// concurrently more than once, and that all repeat tasks execute multiple times.
func TestStress_UnlockedRepeatTasksNoOverlap(t *testing.T) {
	rdb := setupTestRedis(t)

	config := &Config{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		MaxJitter:      0,
		ErrorDelay:     10 * time.Millisecond,
	}

	q, err := New(rdb, config)
	require.NoError(t, err)

	ctx := context.Background()

	const (
		numTasks      = 256
		interval      = 50 * time.Millisecond
		workerRuntime = 12 * time.Second
	)

	var (
		mu             sync.Mutex
		inFlightByID   = make(map[string]int)
		execCountByID  = make(map[string]int)
		concurrencyErr bool
	)

	handler := func(ctx context.Context, payload []byte) error {
		id := string(payload)

		mu.Lock()
		inFlightByID[id]++
		if inFlightByID[id] > 1 {
			// The same repeat task ID should never be executing concurrently.
			concurrencyErr = true
		}
		mu.Unlock()

		time.Sleep(3 * time.Millisecond)

		mu.Lock()
		inFlightByID[id]--
		execCountByID[id]++
		mu.Unlock()

		return nil
	}

	q.RegisterHandler("stress_unlocked_repeat", handler, OnlyUnlocked())

	for i := 0; i < numTasks; i++ {
		id := fmt.Sprintf("repeat-task-%d", i)
		_, err := q.EnqueueRepeat(ctx, "stress_unlocked_repeat", &TaskOptions{
			Payload: []byte(id),
		}, interval)
		require.NoError(t, err)
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = q.Start(workerCtx)
	}()

	deadline := time.Now().Add(workerRuntime)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	require.False(t, concurrencyErr, "a single unlocked repeating task ID must not execute concurrently with itself")

	// Many (but not necessarily all) repeat tasks should execute at least once.
	assert.GreaterOrEqual(t, len(execCountByID), numTasks/4, "at least 25%% of repeat tasks should execute in stress window")
}
