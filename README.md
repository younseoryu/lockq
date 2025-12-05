# lockq

Redis-backed task queue for Go with optional distributed locking.

## Why lockq?

`lockq` gives you a simple way to move slow or unreliable work (emails, webhooks, sync jobs, batch processing, etc.) off your request path into a durable, Redis-backed queue. It lets you control *when* work runs (delayed and repeating tasks), *how often* it is retried (with backoff and DLQ), and *how it is serialized* per key (distributed locking, tasks with the same key never run concurrently), without having to run a separate broker or heavyweight worker framework.

Because all coordination happens through Redis, you can horizontally scale producers and workers across many processes and hosts: they all talk to the same Redis instance or cluster, and the Lua scripts in this package ensure that enqueue, dequeue, locking, and retries are atomic and race-free.

## Architecture at a Glance

At a high level, `lockq` looks like this:

```text
                 +-------------------+
                 |     Producers     |
                 |  (your services)  |
                 +---------+---------+
                           |
                           | Enqueue tasks (immediate,
                           | delayed, repeating, locked)
                           v
        +------------------------------------------------------+
        |                 Redis (single source of truth)       |
        |                                                      |
        |  - Per-type unlocked queues:   q:unlocked:...        |
        |  - Per-type locked queues:     q:locked:...          |
        |  - Task hashes:                t:<id>                |
        |  - Lock keys (per LockKey):    l:<lock_key>          |
        |  - Repeat-lock keys:           rl:<lock_key>         |
        |  - Dead-letter queue:          dlq                   |
        +--------------------------+---------------------------+
                           ^
                 Pop + lock tasks via
                   atomic Lua scripts
                           |
        +------------------+------------------+------------------+
        |     Worker A     |     Worker B     |     Worker C     |
        |  (processes or   |  (same code,     |  (same queues,   |
        |   containers)    |   same queues)   |   more capacity) |
        +------------------+------------------+------------------+
```

This architecture scales horizontally: workers are effectively stateless, keeping only ephemeral in-memory state and delegating all coordination to Redis. You can add or remove worker processes to match load, or run workers alongside your application servers, without changing application logic. Redis and the Lua scripts provide strong guarantees that each task is processed at most once per execution, that per-key locks are respected across all workers, and that retries and DLQ transitions happen atomically.

## Features

- Delayed and repeating tasks
- Distributed locking for serial execution
- Automatic retries with exponential backoff
- Dead letter queue
- Graceful shutdown
- Lifecycle hooks

### Lock Keys and Serial Execution

Tasks can be associated with a **lock key** (for example, `user:123` or `account:42`) via `TaskOptions.LockKey`. All tasks with the same lock key go through the **locked queue** and share a single distributed mutex in Redis, so at most one of them runs at a time across all workers and processes. This lets you safely model “per-entity” workflows (e.g. syncing one user, updating one account, processing one order) without worrying about concurrent races or stale interleaved writes, even when you scale out to many worker processes.

For repeating tasks, `lockq` also keeps a separate repeat lock per lock key so you don’t accidentally enqueue multiple repeating jobs for the same key; only one repeating task per `(type, lock key)` will be accepted, and it will continue to fire on its schedule, one execution at a time, while the lock ensures serial processing.

## Getting Started

Make sure you have Go installed ([download](https://go.dev/dl)). The latest two Go versions are supported.

Initialize your project and install lockq:

```bash
go mod init github.com/your/repo
go get -u github.com/younseoryu/lockq@latest
```

Make sure you have Redis 4.0+ running locally or in Docker.

### Define Tasks

Create a package that defines task types and handlers:

```go
package tasks

import (
    "context"
    "encoding/json"
    "log"
)

const (
    TypeEmailDelivery = "email:deliver"
    TypeUserSync      = "user:sync"
)

type EmailPayload struct {
    UserID     int
    TemplateID string
}

func HandleEmailDelivery(ctx context.Context, payload []byte) error {
    var p EmailPayload
    if err := json.Unmarshal(payload, &p); err != nil {
        return err
    }
    log.Printf("Sending email: user_id=%d, template_id=%s", p.UserID, p.TemplateID)
    return nil
}

func HandleUserSync(ctx context.Context, payload []byte) error {
    log.Printf("Syncing user: %s", payload)
    return nil
}
```

### Enqueue Tasks

Use the queue client to enqueue tasks:

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "time"

    "github.com/go-redis/redis/v8"
    "github.com/younseoryu/lockq"
    "your/app/tasks"
)

func main() {
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    q, _ := lockq.New(rdb, &lockq.Config{})
    ctx := context.Background()

    // Immediate task
    payload, _ := json.Marshal(tasks.EmailPayload{UserID: 42, TemplateID: "welcome"})
    id, _ := q.Enqueue(ctx, tasks.TypeEmailDelivery, &lockq.TaskOptions{
        Payload: payload,
    })
    log.Printf("Enqueued: %s", id)

    // Delayed task (process in 24 hours)
    q.Enqueue(ctx, tasks.TypeEmailDelivery, &lockq.TaskOptions{
        Payload: payload,
        Delay:   24 * time.Hour,
    })

    // Locked task (serial execution per user)
    q.Enqueue(ctx, tasks.TypeUserSync, &lockq.TaskOptions{
        LockKey: "user:123",
        Payload: []byte("123"),
    })

    // Repeating task (runs every hour)
    q.EnqueueRepeat(ctx, tasks.TypeUserSync, &lockq.TaskOptions{
        LockKey: "user:123",
        Payload: []byte("123"),
    }, 1*time.Hour)
}
```

### Start Workers

Start a worker server to process tasks:

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/go-redis/redis/v8"
    "github.com/younseoryu/lockq"
    "your/app/tasks"
)

func main() {
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    q, _ := lockq.New(rdb, &lockq.Config{
        LockTimeout: 30 * time.Second,
        MaxRetries:  3,
    })

    // Register handlers
    q.RegisterHandler(tasks.TypeEmailDelivery, tasks.HandleEmailDelivery)
    q.RegisterHandler(tasks.TypeUserSync, tasks.HandleUserSync, lockq.OnlyLocked())

    log.Println("Starting worker...")
    if err := q.Start(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

## Handler Modes

Control which tasks a handler processes:

```go
q.RegisterHandler("task1", handler, lockq.OnlyLocked())       // locked tasks only
q.RegisterHandler("task2", handler, lockq.OnlyUnlocked())     // unlocked tasks only
q.RegisterHandler("task3", handler, lockq.LockedOrUnlocked()) // both (default)
```

## Retries

Failed tasks retry with quadratic backoff: 1s, 4s, 9s, ... (capped at 5 min). After exhausting retries, tasks move to the dead letter queue. Locks are released during retry delays.

### Behavioral Guarantees and Caveats

- **Per-key serialization depends on lock timeout**: For tasks with a `LockKey`, `lockq` enforces that at most one execution per key holds the Redis lock at a time. This guarantee assumes your handlers complete comfortably within `LockTimeout`; if a handler runs longer than `LockTimeout`, the Redis lock can expire and another worker may start processing work for the same key while the first handler is still running. Configure `LockTimeout` to be greater than the worst‑case handler duration and have handlers respect the passed context.

- **Non-repeating tasks can be lost on worker crash**: When a task is popped for processing, it is removed from the ready queue and its metadata is given a TTL tied to `LockTimeout`. If the worker process crashes or is killed after popping the task but before it either requeues or moves the task to the DLQ, that task will eventually expire and not be retried. In practice this means `lockq` provides at-least-once processing as long as workers fail *before* claiming tasks or *after* recording their outcome; hard crash in the middle of handler execution can result in loss for non-repeating tasks.

## Configuration

```go
&lockq.Config{
    LockTimeout:      30 * time.Second,
    MaxRetries:       3,
    InitialBackoff:   25 * time.Millisecond,
    MaxBackoff:       500 * time.Millisecond,
    MaxJitter:        25 * time.Millisecond,
    ErrorDelay:       1 * time.Second,
    PeekSize:         25,
    FallbackPeekSize: 100,
    KeyPrefix:        "lockq:",
}
```

## Hooks

```go
q.SetTaskStartHook(func(task *lockq.Task) { })
q.SetTaskSuccessHook(func(task *lockq.Task, duration time.Duration) { })
q.SetTaskFailureHook(func(task *lockq.Task, err error, duration time.Duration) { })
q.SetTaskRetryHook(func(task *lockq.Task, err error, duration time.Duration) { })
```

## Queue Inspection

```go
// Count ready tasks
q.Count(ctx, lockq.QueueUnlocked)
q.Count(ctx, lockq.QueueLocked)

// Count all tasks including scheduled
q.CountAll(ctx, lockq.QueueUnlocked)

// Count by task type
q.CountByType(ctx, "email:deliver", lockq.QueueUnlocked)
q.CountAllByType(ctx, "email:deliver", lockq.QueueLocked)

// Dead letter queue
q.GetDeadLetterQueueCount(ctx)
q.RetryFromDLQ(ctx, taskID)

// Delete tasks
q.DeleteTask(ctx, taskID)                   // Delete any task by ID
q.DeleteRepeatTask(ctx, "user:123")         // Stop locked repeating task by lock key
```

## Graceful Shutdown

```go
ctx, cancel := context.WithCancel(context.Background())
go q.Start(ctx)

// On shutdown
cancel()
q.Stop(context.WithTimeout(context.Background(), 30*time.Second))
```

## Releasing a New Version

To release a new version of lockq:

1. **Update the code** and ensure all tests pass:
   ```bash
   go test -race ./...
   ```

2. **Commit your changes**:
   ```bash
   git add .
   git commit -m "Release v1.0.0"
   git push origin main
   ```

3. **Create and push a version tag**:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

4. **GitHub Actions will automatically**:
   - Run tests to verify the release
   - Create a GitHub Release with auto-generated changelog
   - Make the new version available via `go get`

### Versioning Guidelines

- **Patch version** (v1.0.x): Bug fixes, no API changes
- **Minor version** (v1.x.0): New features, backwards compatible
- **Major version** (vx.0.0): Breaking API changes

## License

MIT
