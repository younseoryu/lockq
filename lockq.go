// Package lockq provides a Redis-backed task queue with optional distributed locking.
// It supports delayed execution, task retries, dead letter queues, and repeating tasks.
package lockq

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// LockKeyMode determines whether tasks require distributed locking.
type LockKeyMode int

const (
	// LockKeyBoth processes tasks with or without lock keys.
	LockKeyBoth LockKeyMode = iota
	// LockKeyOnly processes only tasks with lock keys.
	LockKeyOnly
	// LockKeyNever processes only tasks without lock keys.
	LockKeyNever
)

// HandlerConfig configures task handler behavior.
type HandlerConfig struct {
	LockKeyMode LockKeyMode
}

// OnlyLocked configures the handler to only process tasks with lock keys.
func OnlyLocked() *HandlerConfig {
	return &HandlerConfig{
		LockKeyMode: LockKeyOnly,
	}
}

// OnlyUnlocked configures the handler to only process tasks without lock keys.
func OnlyUnlocked() *HandlerConfig {
	return &HandlerConfig{
		LockKeyMode: LockKeyNever,
	}
}

// LockedOrUnlocked configures the handler to process both locked and unlocked tasks.
// This is the default if no config is provided.
func LockedOrUnlocked() *HandlerConfig {
	return &HandlerConfig{
		LockKeyMode: LockKeyBoth,
	}
}

// Queue manages task processing with support for locking, retries, and scheduling.
type Queue struct {
	store          *Store
	config         *Config
	handlers       map[string]HandlerFunc
	handlerConfigs map[string]*HandlerConfig
	knownTaskTypes map[string]struct{}
	mu             sync.RWMutex

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	workerWg sync.WaitGroup

	randSource *rand.Rand
	randMu     sync.Mutex

	hookMu        sync.RWMutex
	onTaskStart   TaskHook
	onTaskSuccess TaskResultHook
	onTaskFailure TaskErrorHook
	onTaskRetry   TaskErrorHook
}

// New creates a new Queue instance backed by Redis.
func New(redis *redis.Client, config *Config) (*Queue, error) {
	store, err := NewStore(redis, config)
	if err != nil {
		return nil, err
	}

	return &Queue{
		store:          store,
		config:         store.config,
		handlers:       make(map[string]HandlerFunc),
		handlerConfigs: make(map[string]*HandlerConfig),
		knownTaskTypes: make(map[string]struct{}),
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
		randSource:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// RegisterHandler registers a task handler for the given task type.
// If no config is provided, defaults to MaybeLock.
func (q *Queue) RegisterHandler(taskType string, handler HandlerFunc, configs ...*HandlerConfig) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.handlers[taskType] = handler
	q.knownTaskTypes[taskType] = struct{}{}

	var config *HandlerConfig
	if len(configs) > 0 {
		config = configs[0]
	} else {
		config = &HandlerConfig{
			LockKeyMode: LockKeyBoth,
		}
	}

	q.handlerConfigs[taskType] = config
}

// Enqueue adds a task to the queue.
// Returns the task ID.
func (q *Queue) Enqueue(ctx context.Context, taskType string, opts *TaskOptions) (string, error) {
	q.mu.Lock()
	q.knownTaskTypes[taskType] = struct{}{}
	q.mu.Unlock()

	task := &Task{
		Type:        taskType,
		Payload:     opts.Payload,
		LockKey:     opts.LockKey,
		ScheduledAt: time.Now().Add(opts.Delay),
		MaxAttempts: q.config.MaxRetries + 1,
		CreatedAt:   time.Now(),
	}

	return q.store.Push(ctx, task)
}

// EnqueueRepeat adds a repeating task that executes at the specified interval.
// Returns the task ID.
func (q *Queue) EnqueueRepeat(ctx context.Context, taskType string, opts *TaskOptions, interval time.Duration) (string, error) {
	if interval <= 0 {
		return "", fmt.Errorf("repeat interval must be positive, got: %v", interval)
	}

	q.mu.Lock()
	q.knownTaskTypes[taskType] = struct{}{}
	q.mu.Unlock()

	task := &Task{
		Type:        taskType,
		Payload:     opts.Payload,
		LockKey:     opts.LockKey,
		Repeat:      interval,
		ScheduledAt: time.Now().Add(opts.Delay),
		MaxAttempts: q.config.MaxRetries + 1,
		CreatedAt:   time.Now(),
	}

	return q.store.Push(ctx, task)
}

// Start begins processing tasks. Blocks until ctx is cancelled.
// Waits for all in-flight tasks to complete before returning.
func (q *Queue) Start(ctx context.Context) error {
	defer close(q.done)

	q.startWorkers(ctx)

	<-ctx.Done()

	q.stopOnce.Do(func() {
		close(q.stop)
	})

	q.workerWg.Wait()

	return ctx.Err()
}

// startWorkers spawns a worker goroutine per (taskType, queueType) pair.
func (q *Queue) startWorkers(ctx context.Context) {
	q.mu.RLock()
	handlerCount := len(q.handlers)
	handlerConfigs := make(map[string]*HandlerConfig, len(q.handlerConfigs))
	for k, v := range q.handlerConfigs {
		handlerConfigs[k] = v
	}
	knownTypes := make([]string, 0, len(q.knownTaskTypes))
	for t := range q.knownTaskTypes {
		knownTypes = append(knownTypes, t)
	}
	q.mu.RUnlock()

	if handlerCount == 0 {
		if len(knownTypes) == 0 {
			log.Printf("[lockq] No handlers registered and no known task types; no workers started")
			return
		}
		log.Printf("[lockq] No handlers registered; starting cleanup workers for known task types")
		for _, taskType := range knownTypes {
			q.workerWg.Add(2)
			go q.workerLoopForType(ctx, taskType, QueueUnlocked)
			go q.workerLoopForType(ctx, taskType, QueueLocked)
		}
		return
	}

	// Start workers per task type based on handler config.
	for taskType, cfg := range handlerConfigs {
		if _, exists := q.handlers[taskType]; !exists {
			continue
		}
		if cfg == nil {
			cfg = LockedOrUnlocked()
		}

		if cfg.LockKeyMode == LockKeyNever || cfg.LockKeyMode == LockKeyBoth {
			q.workerWg.Add(1)
			go q.workerLoopForType(ctx, taskType, QueueUnlocked)
		}
		if cfg.LockKeyMode == LockKeyOnly || cfg.LockKeyMode == LockKeyBoth {
			q.workerWg.Add(1)
			go q.workerLoopForType(ctx, taskType, QueueLocked)
		}
	}
}

// workerLoopForType runs a polling loop for a specific task type and queue type.
func (q *Queue) workerLoopForType(ctx context.Context, taskType string, queueType QueueType) {
	defer q.workerWg.Done()

	queueName := "unlocked"
	if queueType == QueueLocked {
		queueName = "locked"
	}

	log.Printf("[lockq] Worker started for %s queue (type=%s)", queueName, taskType)

	backoff := q.config.InitialBackoff

	for {
		select {
		case <-q.stop:
			log.Printf("[lockq] Worker stopped for %s queue (type=%s)", queueName, taskType)
			return
		default:
		}

		hasTask, err := q.pollAndProcessForType(ctx, taskType, queueType)
		if err != nil {
			log.Printf("[lockq] Error polling %s queue (type=%s): %v", queueName, taskType, err)
			select {
			case <-q.stop:
				log.Printf("[lockq] Worker stopped for %s queue (type=%s)", queueName, taskType)
				return
			case <-time.After(q.config.ErrorDelay):
			}
			continue
		}

		if hasTask {
			backoff = q.config.InitialBackoff
		} else {
			jitter := q.getJitter()

			select {
			case <-q.stop:
				log.Printf("[lockq] Worker stopped for %s queue (type=%s)", queueName, taskType)
				return
			case <-time.After(backoff + jitter):
				backoff *= 2
				if backoff > q.config.MaxBackoff {
					backoff = q.config.MaxBackoff
				}
			}
		}
	}
}

func (q *Queue) pollAndProcessForType(ctx context.Context, taskType string, queueType QueueType) (bool, error) {
	var (
		task *Task
		err  error
	)

	if queueType == QueueUnlocked {
		task, err = q.store.PopUnlocked(ctx, taskType)
	} else {
		task, err = q.store.PopLocked(ctx, taskType)
	}

	if err != nil {
		return false, err
	}

	if task == nil {
		return false, nil
	}

	q.processTask(task)

	return true, nil
}

func (q *Queue) processTask(task *Task) {
	handlerTimeout := q.config.LockTimeout - 5*time.Second
	if handlerTimeout < 5*time.Second {
		handlerTimeout = q.config.LockTimeout * 9 / 10
	}
	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()

	shouldCleanupTask := true
	startTime := time.Now()

	q.hookMu.RLock()
	hookStart := q.onTaskStart
	q.hookMu.RUnlock()
	if hookStart != nil {
		hookStart(task)
	}

	defer func() {
		if shouldCleanupTask {
			if err := q.store.Unlock(context.Background(), task); err != nil {
				log.Printf("[lockq] Error unlocking task %s: %v", task.ID, err)
			}
		}
	}()

	q.mu.RLock()
	handler, ok := q.handlers[task.Type]
	q.mu.RUnlock()

	if !ok {
		log.Printf("[lockq] No handler registered for task type: %s", task.Type)

		if task.IsRepeat() {
			if err := q.store.DeleteTask(context.Background(), task.ID); err != nil {
				log.Printf("[lockq] Error deleting unhandled repeat task %s: %v", task.ID, err)
			}
		}
		return
	}

	err := handler(ctx, task.Payload)

	if err != nil {
		if task.Attempts+1 < task.MaxAttempts {
			log.Printf("[lockq] Task %s (type: %s) failed (attempt %d/%d): %v - will retry",
				task.ID, task.Type, task.Attempts+1, task.MaxAttempts, err)

			q.hookMu.RLock()
			hookRetry := q.onTaskRetry
			q.hookMu.RUnlock()
			if hookRetry != nil {
				hookRetry(task, err, time.Since(startTime))
			}

			retryDelay := time.Duration((task.Attempts+1)*(task.Attempts+1)) * time.Second
			if retryDelay > 5*time.Minute {
				retryDelay = 5 * time.Minute
			}

			if requeueErr := q.store.Requeue(context.Background(), task, retryDelay); requeueErr != nil {
				log.Printf("[lockq] Failed to requeue task %s: %v", task.ID, requeueErr)
				if dlqErr := q.store.MoveToDeadLetter(context.Background(), task, err); dlqErr != nil {
					log.Printf("[lockq] Failed to move task %s to DLQ: %v", task.ID, dlqErr)
				} else {
					shouldCleanupTask = false
				}
			} else {
				shouldCleanupTask = false
				if err := q.store.ReleaseLock(context.Background(), task); err != nil {
					log.Printf("[lockq] Error releasing lock for task %s: %v", task.ID, err)
				}
			}
		} else {
			log.Printf("[lockq] Task %s (type: %s) failed permanently after %d attempts: %v",
				task.ID, task.Type, task.Attempts+1, err)

			q.hookMu.RLock()
			hookFailure := q.onTaskFailure
			q.hookMu.RUnlock()
			if hookFailure != nil {
				hookFailure(task, err, time.Since(startTime))
			}

			if dlqErr := q.store.MoveToDeadLetter(context.Background(), task, err); dlqErr != nil {
				log.Printf("[lockq] Failed to move task %s to DLQ: %v", task.ID, dlqErr)
			}
			shouldCleanupTask = false
		}
		return
	}

	q.hookMu.RLock()
	hookSuccess := q.onTaskSuccess
	q.hookMu.RUnlock()
	if hookSuccess != nil {
		hookSuccess(task, time.Since(startTime))
	}

	if task.IsRepeat() {
		if _, err := q.store.ResetAttempts(context.Background(), task.ID, task.ExecutionID); err != nil {
			log.Printf("[lockq] Error resetting attempts for repeat task %s: %v", task.ID, err)
		}
	}
}

// Stop gracefully stops the queue. Blocks until all workers finish or ctx is cancelled.
func (q *Queue) Stop(ctx context.Context) error {
	q.stopOnce.Do(func() {
		close(q.stop)
	})

	select {
	case <-q.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *Queue) getJitter() time.Duration {
	if q.config.MaxJitter == 0 {
		return 0
	}

	q.randMu.Lock()
	defer q.randMu.Unlock()

	return time.Duration(q.randSource.Int63n(int64(q.config.MaxJitter)))
}

// SetTaskStartHook sets a callback invoked when a task starts processing.
func (q *Queue) SetTaskStartHook(hook TaskHook) {
	q.hookMu.Lock()
	defer q.hookMu.Unlock()
	q.onTaskStart = hook
}

// SetTaskSuccessHook sets a callback invoked when a task completes successfully.
func (q *Queue) SetTaskSuccessHook(hook TaskResultHook) {
	q.hookMu.Lock()
	defer q.hookMu.Unlock()
	q.onTaskSuccess = hook
}

// SetTaskFailureHook sets a callback invoked when a task fails permanently.
func (q *Queue) SetTaskFailureHook(hook TaskErrorHook) {
	q.hookMu.Lock()
	defer q.hookMu.Unlock()
	q.onTaskFailure = hook
}

// SetTaskRetryHook sets a callback invoked when a task is retried.
func (q *Queue) SetTaskRetryHook(hook TaskErrorHook) {
	q.hookMu.Lock()
	defer q.hookMu.Unlock()
	q.onTaskRetry = hook
}

// RetryFromDLQ moves a failed task from the dead letter queue back to processing.
func (q *Queue) RetryFromDLQ(ctx context.Context, taskID string) error {
	return q.store.RetryFromDLQ(ctx, taskID)
}

// Count returns the number of tasks that are ready to be processed right now.
// This excludes tasks scheduled for future execution.
func (q *Queue) Count(ctx context.Context, queueType QueueType) (int64, error) {
	q.mu.RLock()
	types := make([]string, 0, len(q.knownTaskTypes))
	for t := range q.knownTaskTypes {
		types = append(types, t)
	}
	q.mu.RUnlock()

	var total int64
	for _, taskType := range types {
		var queueKey string
		if queueType == QueueUnlocked {
			queueKey = q.config.UnlockedQueueKeyFor(taskType)
		} else {
			queueKey = q.config.LockedQueueKeyFor(taskType)
		}

		n, err := q.store.Count(ctx, queueKey, 0)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// CountAll returns the total number of tasks in the queue,
// including both ready tasks and tasks scheduled for future execution.
func (q *Queue) CountAll(ctx context.Context, queueType QueueType) (int64, error) {
	q.mu.RLock()
	types := make([]string, 0, len(q.knownTaskTypes))
	for t := range q.knownTaskTypes {
		types = append(types, t)
	}
	q.mu.RUnlock()

	var total int64
	for _, taskType := range types {
		var queueKey string
		if queueType == QueueUnlocked {
			queueKey = q.config.UnlockedQueueKeyFor(taskType)
		} else {
			queueKey = q.config.LockedQueueKeyFor(taskType)
		}

		n, err := q.store.CountAll(ctx, queueKey)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// CountByType returns the number of ready tasks for a specific task type.
func (q *Queue) CountByType(ctx context.Context, taskType string, queueType QueueType) (int64, error) {
	var queueKey string
	if queueType == QueueUnlocked {
		queueKey = q.config.UnlockedQueueKeyFor(taskType)
	} else {
		queueKey = q.config.LockedQueueKeyFor(taskType)
	}
	return q.store.Count(ctx, queueKey, 0)
}

// CountAllByType returns the total number of tasks for a specific task type.
func (q *Queue) CountAllByType(ctx context.Context, taskType string, queueType QueueType) (int64, error) {
	var queueKey string
	if queueType == QueueUnlocked {
		queueKey = q.config.UnlockedQueueKeyFor(taskType)
	} else {
		queueKey = q.config.LockedQueueKeyFor(taskType)
	}
	return q.store.CountAll(ctx, queueKey)
}

// DeleteTask permanently removes a task from all queues.
func (q *Queue) DeleteTask(ctx context.Context, taskID string) error {
	return q.store.DeleteTask(ctx, taskID)
}

// GetDeadLetterQueueCount returns the number of permanently failed tasks.
func (q *Queue) GetDeadLetterQueueCount(ctx context.Context) (int64, error) {
	dlqKey := q.config.DeadLetterQueueKey()
	return q.store.redis.ZCard(ctx, dlqKey).Result()
}
