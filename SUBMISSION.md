# Submission, Real-time Streaming Backend

**Your name:** Simon Liska
**Email:** simon.liska@gmail.com
**Link to your fork or solution:** https://github.com/simonliska/careers-challenge-streaming-backend

---

## Stack and storage

One Go program, standard library only. Events come in over HTTP, state lives in memory for fast reads, and every event hits an append-only log on disk first, so nothing acknowledged is ever lost. A JSON snapshot every 60s keeps restarts fast. No Kafka, no database (tried to keep it simple).

## How it fits together

`POST /events` validates, writes the log, and hands the event to one of 64 workers chosen by hashing the device id, so each device keeps its order. Workers update device health, room presence, and the fall alarm list. Reads are plain HTTP: health, occupancy (1m/5m/1h), alarms, live alarm stream (SSE), `/metrics`. Folders in `service/`: `cmd/server` wiring; `internal/httpapi` endpoints; `internal/ingest` workers; `internal/store` state; `internal/wal` log; `internal/domain` types.

## Ordering and late events

Everything keys on the device timestamp, never arrival order. A fall is identified by device + exact timestamp: sensor-jitter copies collapse into one, a real second fall (different timestamp) is kept. Timestamps up to 1h old are accepted for offline replay; more than 1h in the future is rejected.

## Backpressure

Each worker has two queues: priority for falls, bulk for the rest. Full queues make the sender wait - delay, never drops, no 429s. Falls skip past bulk backlogs, so alarm latency stays flat in a burst.

## Restart correctness

Log first, acknowledge second. A restart replays snapshot plus log tail and rebuilds the same state; verified with a hard kill mid-burst.

## Why the generator changed

The stock sender uses one thread (~1–1.6k events/s) and tops out long before a 5k-device burst (~15k/s), so full load was untestable. I added opt-in `--workers N`: threads with keep-alive connections, routed per device to keep order. Default path and server contract untouched.

## How I worked

Built with OpenCode with the Muse Spark 1.3 model, in 4 sessions (~10–13h total): ingest and aggregations first, then persistence, backpressure, hardening - each step verified live before moving on. Go tests cover shard priority, fall dedup, late replay, WAL restore, and drop counting.

## How to run it locally

```bash
cd service && go build -o /tmp/teton-service ./cmd/server && ADDR=:9090 /tmp/teton-service  # wipe ./data first for a clean run
# repo root:
python3 eval/check.py burst --target http://localhost:9090 --devices 5000 --workers 16
curl -s http://localhost:9090/metrics | python3 -m json.tool
```

## Reported metrics

- Sustained ingest rate: ~6.4k eps avg over 180s burst run (~15k in-burst)
- Alarm feed latency p50 / p95: ingest→wire p50 2.4ms / p95 5.0ms.
- Behavior under hard kill + restart: full state restored from WAL + snapshot, counts exact, including a mid-burst `kill -9`.

Eval note: `fall_warn_total` in `/metrics` counts distinct post-dedup falls; `broadcast_dropped` counts live deliveries skipped into a full subscriber buffer (stored alarms unaffected).

## With another week

- Proper error codes on the API (today it is plain ok/error strings, thin for operators) and safe retries - resending failed requests while the server stays idempotent, so the few bulk requests lost in the offline-flush second disappear instead of just being counted.
