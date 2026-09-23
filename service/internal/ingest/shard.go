package ingest

import (
	"hash/fnv"
	"time"

	"teton-service/internal/domain"
)

type Applier interface {
	Apply(ev domain.Event, now time.Time) (bool, string)
}

// Shards routes each device_id to a fixed worker, so events from one
// device are applied sequentially without a global lock.
type Shards struct {
	store   Applier
	prio    []chan domain.Event
	bulk    []chan domain.Event
	nowFunc func() time.Time
}

// New starts nShards workers, each draining a prio and a bulk queue.
// The worker always drains prio first, so falls skip bulk backlogs.
func New(st Applier, nShards, prioSize, bulkSize int) *Shards {
	sh := &Shards{
		store:   st,
		prio:    make([]chan domain.Event, nShards),
		bulk:    make([]chan domain.Event, nShards),
		nowFunc: time.Now,
	}
	for i := range sh.prio {
		sh.prio[i] = make(chan domain.Event, prioSize)
		sh.bulk[i] = make(chan domain.Event, bulkSize)
		go sh.loop(sh.prio[i], sh.bulk[i])
	}
	return sh
}

func (sh *Shards) loop(prio, bulk chan domain.Event) {
	for {
		select {
		case ev := <-prio:
			sh.store.Apply(ev, sh.nowFunc())
			continue
		default:
		}
		select {
		case ev := <-prio:
			sh.store.Apply(ev, sh.nowFunc())
		case ev := <-bulk:
			sh.store.Apply(ev, sh.nowFunc())
		}
	}
}

// Enqueue blocks when the shard queue is full, pushing back on HTTP
// instead of dropping events. Falls go to prio, everything else to bulk.
func (sh *Shards) Enqueue(ev domain.Event) {
	i := shardOf(ev.DeviceID, len(sh.prio))
	if domain.IsPriority(ev.Type) {
		sh.prio[i] <- ev
		return
	}
	sh.bulk[i] <- ev
}

func shardOf(deviceID string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(deviceID))
	return int(h.Sum32() % uint32(n))
}
