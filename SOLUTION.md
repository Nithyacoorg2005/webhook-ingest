# Backend Intern Assignment - Solution

## 1. What was broken, and why
The operations team reported three distinct symptoms, each mapping to a specific defect:
- Call-counts drifting higher:  The in-memory stats cache (`internal/stats/cache.go`) was suffering from a classic race condition. The `Record` function was updating the map and incrementing integers concurrently without a mutex. I added a `sync.Mutex` lock to serialize access.
- Recordings failing silently:  The asynchronous `processRecording` goroutine was silently swallowing errors (`// TODO: handle`). Worse, the HTTP request context was being passed into the background goroutine. When the HTTP request finished, the context was canceled, instantly killing the recording download. I replaced it with `context.Background()` and added structured logging for failures.
-  In-flight requests disappearing on deploy:  The Graceful Shutdown in `main.go` only waited for HTTP connections (`srv.Shutdown`), totally ignoring detached background goroutines. I implemented a `sync.WaitGroup` in the ingest service to track active background tasks and forced `main.go` to wait for them to complete cleanly before exiting.

## 2. Idempotency Strategy (Postgres vs. Redis)
I chose  Postgres  (Database-level `UNIQUE` constraint) over Redis for deduplication. 
The application originally utilized an application-level "check-then-act" pattern (`EventExists` followed by `InsertEvent`), which is prone to race conditions if two identical webhooks arrive simultaneously. By adding a `UNIQUE` constraint on `event_id` in PostgreSQL and modifying the insert to use `ON CONFLICT DO NOTHING`, idempotency is guaranteed natively by the storage layer. I avoided Redis to minimize distributed state complexity; since Postgres is already the source of truth, enforcing it there prevents split-brain scenarios and ensures absolute accuracy without adding another failure point.

## 3. Scaling to 10,000 webhooks/sec
If the system had to ingest 10,000 requests per second, direct synchronous writes to Postgres on every HTTP request would immediately bottleneck the connection pool and cause severe latency.
I would change the architecture to decouple ingestion from processing:
1.  Message Queue:  The HTTP handlers would immediately drop incoming payloads onto a distributed message broker (e.g., Kafka or RabbitMQ) and return a `200 OK`. 
2.  Batch Processing Workers:  A pool of background worker services would consume events from the queue and perform batch database inserts (using bulk `COPY` or batched `Upsert` queries) to drastically reduce database transaction overhead. 
3.  Redis Caching:  Redis could be heavily utilized to buffer incoming hit counts, flushing the aggregates to the PostgreSQL `account_stats` table asynchronously (e.g., every 5 seconds) rather than hitting the DB for every single call.