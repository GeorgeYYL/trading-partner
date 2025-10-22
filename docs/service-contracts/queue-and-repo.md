Queue & Job Repo — Service Contract (v1)
1. Purpose and Scope

Purpose: Define the interaction rules between API/Scheduler and Queue/Job Repo for the daily_pipeline job lifecycle — from enqueue → execute → store → status update.

Out of Scope:

HTTP interface details (handled in api/schemas).

Internal Worker logic (fetch, transform, quality check, storage).

2. Shared Assumptions

Message schema: libs/contracts.JobMessage, JobStatus, and make_idempotency_key.

Timezone: UTC.

Delivery semantics: Queue guarantees at-least-once delivery; Worker must be idempotent.

Tracing fields: Every log or metric must include
job_id, idempotency_key, symbol, and asof.

3. Queue Contract
3.1 Interface

enqueue(message: JobMessage) → EnqueueResult

Success: { message_id: str, visible_at: datetime }

Retryable failure: error_code = QUEUE_UNAVAILABLE

Non-retryable failures: MESSAGE_TOO_LARGE, INVALID_MESSAGE

ping() → ok | error(reason: str)
Used by /ready; reason examples: auth_failed | timeout | dns_error.

3.2 Semantics

Delivery: At-least-once (possible duplicates). Deduplication handled via idempotency_key.

Visibility timeout: 30 s (default). After a Worker receives a message, it remains invisible to others for 30 s. If not acknowledged, it becomes visible again.

Dead-letter queue (DLQ): A message moved to DLQ after max_attempts = 5.

3.3 Failure Matrix
Scenario	Returned Code	Producer Action
Queue endpoint unreachable	QUEUE_UNAVAILABLE	Exponential back-off retry
Message too large	MESSAGE_TOO_LARGE	Log & alert; stop retry
Message violates data contract	INVALID_MESSAGE	Log & alert; fix producer; stop retry
4. Job Repo Contract
4.1 Interface

create_queued(job_id, idempotency_key, job_type, symbol, asof, requested_by, created_at) → { job_id, created: bool }

If the same idempotency_key already exists and status ∈ {queued, running, succeeded}, return existing job_id (created = false).

set_running(job_id, started_at) → None

set_succeeded(job_id, finished_at, result_ref?) → None

set_failed(job_id, finished_at, error_code, error_message, attempt) → None

set_dead_letter(job_id, finished_at, error_code, error_message, attempt) → None

get_by_id(job_id) → JobRecord (status, attempts, timestamps, last_error, result_ref)

get_by_idempotency(idempotency_key) → job_id?

ping() → ok | error(reason) (for /ready)

4.2 State Machine & Invariants
queued → running → { succeeded | failed } → dead_letter (optional)


(job_type, symbol, asof) → unique idempotency_key; only one active job at a time.

States cannot regress (succeeded → running forbidden).

Every failure must include error_code (short enum) and attempt (count).

4.3 Error Codes
Code	Meaning	Likely Cause
IDEMPOTENCY_CONFLICT	Same key already active/completed	Duplicate submission
UPSTREAM_EMPTY	No data fetched	Data/business
TRANSFORM_INVALID	Invalid cleaned structure	Logic
QUALITY_FAILED	Quality check failed	Data
STORAGE_WRITE_FAILED	Storage write error	Infrastructure
REPO_UNAVAILABLE	Repo unreachable	Infrastructure
5. Interaction Flow
5.1 Enqueue (API/Scheduler Path)

Generate idempotency_key = make_idempotency_key(job_type, symbol, asof).

repo.get_by_idempotency(key)

Found → return existing job_id.

Not found → repo.create_queued(..., created = true) → new job_id.

Call queue.enqueue(JobMessage).

Success → return 202 Accepted + job_id.

Failure → handle per failure matrix (§ 3.3).

5.2 Consume (Worker Path)

Consume message → validate against data contract.

repo.set_running(job_id).

Execute pipeline (fetch → transform → quality → storage).

Success → repo.set_succeeded(job_id, result_ref).

Failure → repo.set_failed(job_id, error_code, error_message, attempt);
if attempts ≥ max_attempts, move to DLQ and repo.set_dead_letter.

6. /ready Health Check

Return 200 only if queue.ping() == ok and repo.ping() == ok.
Otherwise return 503 with body:
{ queue: ok|error(reason), repo: ok|error(reason), timestamp: ... }.

7. Acceptance Scenarios

Idempotent triple submit: Same key 3× → first created =true, next 2 return existing job_id.

Worker crash: Message reappears after visibility timeout; attempt++; moves to DLQ after 5 failures.

Empty upstream: Worker raises UPSTREAM_EMPTY → failed status, no storage write.

Repeat job: Same job re-executed → storage remains deduplicated.

Queue down: enqueue returns QUEUE_UNAVAILABLE; API logs and retries.

/ready: Turn off Queue or Repo → returns 503; restore → 200.

8. Metrics & Auditing

Key metrics:
enqueue_qps, queue_backlog, job_latency_p95, fail_rate, idempotency_conflicts, dlq_size.

Required log fields:
job_id, idempotency_key, symbol, asof, status, error_code?, attempt?.

Security:
No credentials or PII in messages. Error messages ≤ 512 chars. Apply standard redaction.