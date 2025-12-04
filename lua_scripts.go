package lockq

import "fmt"

// Redis hash field names for task data.
const (
	taskFieldType        = "ty"
	taskFieldPayload     = "pl"
	taskFieldLockKey     = "lk"
	taskFieldRepeat      = "rp"
	taskFieldAttempts    = "at"
	taskFieldMaxAttempts = "ma"
	taskFieldExecutionID = "ex"
)

// pushUnlocked creates an unlocked task.
var pushUnlocked = fmt.Sprintf(`
local task_id = ARGV[1]
local task_key = ARGV[6]..task_id
local repeat_ms = tonumber(ARGV[5]) or 0

redis.call('HSET', task_key, '%s', ARGV[2], '%s', ARGV[3], '%s', ARGV[7])
if repeat_ms > 0 then
	redis.call('HSET', task_key, '%s', ARGV[5])
end

local t = redis.call('TIME')
local score = t[1] * 1000000 + t[2] + tonumber(ARGV[4]) * 1000
redis.call('ZADD', KEYS[1], score, task_id)

return task_id
`,
	taskFieldType, taskFieldPayload, taskFieldMaxAttempts,
	taskFieldRepeat,
)

// pushLocked creates a locked task. Rejects duplicate repeating tasks.
var pushLocked = fmt.Sprintf(`
local task_id = ARGV[1]
local repeat_ms = tonumber(ARGV[6]) or 0

if repeat_ms > 0 and ARGV[5] ~= '' then
	local repeat_lock = ARGV[8]..ARGV[5]
	if not redis.call('SET', repeat_lock, task_id, 'NX') then
		return false
	end
end

local task_key = ARGV[7]..task_id
redis.call('HSET', task_key, '%s', ARGV[2], '%s', ARGV[3], '%s', ARGV[5], '%s', ARGV[9])
if repeat_ms > 0 then
	redis.call('HSET', task_key, '%s', ARGV[6])
end

local t = redis.call('TIME')
local score = t[1] * 1000000 + t[2] + tonumber(ARGV[4]) * 1000
redis.call('ZADD', KEYS[1], score, task_id)

return task_id
`,
	taskFieldType, taskFieldPayload, taskFieldLockKey, taskFieldMaxAttempts,
	taskFieldRepeat,
)

// popUnlocked retrieves and processes an unlocked task.
var popUnlocked = fmt.Sprintf(`
local t = redis.call('TIME')
local now = t[1] * 1000000 + t[2]

local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 1)
if #items == 0 then
	return false
end

local task_id = items[1]
local task_key = ARGV[1]..task_id

local ty, pl, rp, at, mr = unpack(redis.call('HMGET', task_key, '%s', '%s', '%s', '%s', '%s'))
local repeat_ms = tonumber(rp) or 0
local attempts = tonumber(at) or 0
local max_retries = tonumber(mr) or 3

local exec_id = t[1]..'-'..t[2]..'-'..task_id
redis.call('HSET', task_key, '%s', exec_id)

redis.call('HINCRBY', task_key, '%s', 1)

if repeat_ms > 0 then
	redis.call('ZADD', KEYS[1], 'XX', now + repeat_ms * 1000, task_id)
else
	redis.call('ZREM', KEYS[1], task_id)
	redis.call('PEXPIRE', task_key, ARGV[2])
end

return {task_id, ty or '', pl or '', '', repeat_ms, attempts, max_retries, exec_id}
`,
	taskFieldType, taskFieldPayload, taskFieldRepeat, taskFieldAttempts, taskFieldMaxAttempts,
	taskFieldExecutionID,
	taskFieldAttempts,
)

// popLocked retrieves a locked task if its lock is available.
var popLocked = fmt.Sprintf(`
local t = redis.call('TIME')
local now = t[1] * 1000000 + t[2]
local max_score = now

local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', max_score, 'LIMIT', 0, ARGV[1])

for i = 1, #items do
	local task_id = items[i]
	local task_key = ARGV[5]..task_id
	
	local ty, pl, lk, rp, at, mr = unpack(redis.call('HMGET', task_key, '%s', '%s', '%s', '%s', '%s', '%s'))
	local lock_key = lk or ''
	local repeat_ms = tonumber(rp) or 0
	local attempts = tonumber(at) or 0
	local max_retries = tonumber(mr) or 3
	
	if lock_key == '' then
		redis.call('ZREM', KEYS[1], task_id)
		redis.call('DEL', task_key)
	else
		local exec_id = t[1]..'-'..t[2]..'-'..task_id
		if redis.call('SET', ARGV[3]..lock_key, exec_id, 'PX', ARGV[4], 'NX') then
			redis.call('HSET', task_key, '%s', exec_id)
			
			redis.call('HINCRBY', task_key, '%s', 1)
			
			if repeat_ms > 0 then
				redis.call('ZADD', KEYS[1], 'XX', now + repeat_ms * 1000, task_id)
			else
				redis.call('ZREM', KEYS[1], task_id)
				redis.call('PEXPIRE', task_key, ARGV[4])
			end
			
			return {task_id, ty or '', pl or '', lock_key, repeat_ms, attempts, max_retries, exec_id}
		end
	end
end

local fallback_size = tonumber(ARGV[2]) or 0
if fallback_size > 0 and fallback_size > tonumber(ARGV[1]) then
	items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', max_score, 'LIMIT', 0, ARGV[2])
	
	for i = 1, #items do
		local task_id = items[i]
		local task_key = ARGV[5]..task_id
		
		local ty, pl, lk, rp, at, mr = unpack(redis.call('HMGET', task_key, '%s', '%s', '%s', '%s', '%s', '%s'))
		local lock_key = lk or ''
		local repeat_ms = tonumber(rp) or 0
		local attempts = tonumber(at) or 0
		local max_retries = tonumber(mr) or 3
		
		if lock_key == '' then
			redis.call('ZREM', KEYS[1], task_id)
			redis.call('DEL', task_key)
		else
			local exec_id = t[1]..'-'..t[2]..'-'..task_id
			if redis.call('SET', ARGV[3]..lock_key, exec_id, 'PX', ARGV[4], 'NX') then
				redis.call('HSET', task_key, '%s', exec_id)
				
				redis.call('HINCRBY', task_key, '%s', 1)
				
				if repeat_ms > 0 then
					redis.call('ZADD', KEYS[1], 'XX', now + repeat_ms * 1000, task_id)
				else
					redis.call('ZREM', KEYS[1], task_id)
					redis.call('PEXPIRE', task_key, ARGV[4])
				end
				
				return {task_id, ty or '', pl or '', lock_key, repeat_ms, attempts, max_retries, exec_id}
			end
		end
	end
end

return false
`,
	taskFieldType, taskFieldPayload, taskFieldLockKey, taskFieldRepeat, taskFieldAttempts, taskFieldMaxAttempts,
	taskFieldExecutionID,
	taskFieldAttempts,
	taskFieldType, taskFieldPayload, taskFieldLockKey, taskFieldRepeat, taskFieldAttempts, taskFieldMaxAttempts,
	taskFieldExecutionID,
	taskFieldAttempts,
)

// unlock releases locks and cleans up completed tasks.
var unlock = fmt.Sprintf(`
local exec_id = ARGV[4]
local rp, lk = unpack(redis.call('HMGET', KEYS[1], '%s', '%s'))
local repeat_ms = tonumber(rp) or 0

if repeat_ms == 0 then
	redis.call('DEL', KEYS[1])
end

if lk and lk ~= '' then
	local lock_key = ARGV[1]..lk
	if redis.call('GET', lock_key) == exec_id then
		redis.call('DEL', lock_key)
	end
end

return true
`,
	taskFieldRepeat, taskFieldLockKey,
)

// count returns the number of tasks ready within a time window.
var count = `
local t = redis.call('TIME')
local now = t[1] * 1000000 + t[2]
return redis.call('ZCOUNT', KEYS[1], '-inf', now + tonumber(ARGV[1]) * 1000)
`

// resetAttempts resets attempts counter if execution_id matches.
var resetAttempts = fmt.Sprintf(`
local task_exists = redis.call('EXISTS', KEYS[1])
if task_exists == 0 then
	return false
end

local current_exec_id = redis.call('HGET', KEYS[1], '%s')
if current_exec_id ~= ARGV[1] then
	return false
end

redis.call('HSET', KEYS[1], 'at', 0)
return true
`, taskFieldExecutionID)

// releaseLock releases a lock only if owned by the current execution.
var releaseLock = `
local lock_key = KEYS[1]
local exec_id = ARGV[1]

if redis.call('GET', lock_key) == exec_id then
	return redis.call('DEL', lock_key)
end
return 0
`

// moveToDLQ moves a failed task to the dead letter queue.
var moveToDLQ = fmt.Sprintf(`
local task_id = ARGV[1]
local queue_key = ARGV[2]
local dlq_key = ARGV[3]
local lock_prefix = ARGV[4]
local repeat_lock_prefix = ARGV[5]
local error_msg = ARGV[6]
local failed_at = ARGV[7]
local is_repeat = ARGV[8] == "1"
local is_locked = ARGV[9] == "1"
local exec_id = ARGV[10]

local lk = redis.call('HGET', KEYS[1], '%s')

if is_repeat then
	redis.call('ZREM', queue_key, task_id)
	if is_locked and lk and lk ~= '' then
		redis.call('DEL', repeat_lock_prefix..lk)
	end
end

redis.call('PERSIST', KEYS[1])
redis.call('HSET', KEYS[1], 'error', error_msg, 'failed_at', failed_at)
redis.call('ZADD', dlq_key, failed_at, task_id)

if is_locked and lk and lk ~= '' then
	local lock_key = lock_prefix..lk
	if redis.call('GET', lock_key) == exec_id then
		redis.call('DEL', lock_key)
	end
end

return true
`,
	taskFieldLockKey,
)

// deleteTask removes a task and all associated data.
var deleteTask = fmt.Sprintf(`
local task_id = ARGV[1]
local lk, rp = unpack(redis.call('HMGET', KEYS[1], '%s', '%s'))

redis.call('ZREM', ARGV[2], task_id)
redis.call('ZREM', ARGV[3], task_id)
redis.call('ZREM', ARGV[4], task_id)
redis.call('DEL', KEYS[1])

if lk and lk ~= '' then
	local lock_key = ARGV[5]..lk
	local lock_val = redis.call('GET', lock_key)
	if lock_val and string.sub(lock_val, -(#task_id + 1)) == '-'..task_id then
		redis.call('DEL', lock_key)
	end
	if rp and rp ~= '' then
		redis.call('DEL', ARGV[6]..lk)
	end
end

return true
`,
	taskFieldLockKey, taskFieldRepeat,
)

// retryFromDLQ moves a task from DLQ back to processing queue.
var retryFromDLQ = fmt.Sprintf(`
local task_id = ARGV[1]
local dlq_key = ARGV[2]
local queue_locked = ARGV[3]
local queue_unlocked = ARGV[4]

if redis.call('EXISTS', KEYS[1]) == 0 then
	return {err = "task not found"}
end

local lk, rp = unpack(redis.call('HMGET', KEYS[1], '%s', '%s'))
local repeat_ms = tonumber(rp) or 0

if repeat_ms > 0 and lk and lk ~= '' then
	local repeat_lock_prefix = ARGV[5]
	if not redis.call('SET', repeat_lock_prefix..lk, task_id, 'NX') then
		return {err = "repeat lock already exists for lock key"}
	end
end

redis.call('PERSIST', KEYS[1])
redis.call('HSET', KEYS[1], '%s', 0)
redis.call('HDEL', KEYS[1], 'error', 'failed_at')
redis.call('ZREM', dlq_key, task_id)

local t = redis.call('TIME')
local score = t[1] * 1000000 + t[2]

if lk and lk ~= '' then
	redis.call('ZADD', queue_locked, score, task_id)
else
	redis.call('ZADD', queue_unlocked, score, task_id)
end

return true
`,
	taskFieldLockKey, taskFieldRepeat, taskFieldAttempts,
)

// requeue persists a task and re-schedules it in the queue.
var requeue = `
local queue_key = KEYS[1]
local task_key = KEYS[2]
local score_us = tonumber(ARGV[1])
local task_id = ARGV[2]

redis.call('PERSIST', task_key)
redis.call('ZADD', queue_key, score_us, task_id)

return true
`
