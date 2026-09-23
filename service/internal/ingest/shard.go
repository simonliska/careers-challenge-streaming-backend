package ingest

import (
	"hash/fnv"
	"time"

	"teton-service/internal/domain"
	"teton-service/internal/store"
)

// Shards routes each device_id to a fixed worker, so events from one
// device are applied sequentially without a global lock.
type Shards struct {
	store   *store.Store
	queues  []chan domain.Event
	nowFunc func() time.Time
}

// New starts nShards workers, each draining a queue of queueSize.
func New(st *store.Store, nShards, queueSize int) *Shards {
	sh := &Shards{store: st, queues: make([]chan domain.Event, nShards), nowFunc: time.Now}
	for i := range sh.queues {
		sh.queues[i] = make(chan domain.Event, queueSize)
		go sh.loop(sh.queues[i])
	}
	return sh
}

func (sh *Shards) loop(q chan domain.Event) {
	for ev := range q {
		sh.store.Apply(ev, sh.nowFunc())
	}
}

// Enqueue blocks when the shard queue is full, pushing back on HTTP
// instead of dropping events.
func (sh *Shards) Enqueue(ev domain.Event) {
	sh.queues[shardOf(ev.DeviceID, len(sh.queues))] <- ev
}

func shardOf(deviceID string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(deviceID))
	return int(h.Sum32() % uint32(n))
}
