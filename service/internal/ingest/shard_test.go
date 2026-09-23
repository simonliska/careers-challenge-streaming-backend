package ingest

import (
	"sync"
	"testing"
	"time"

	"teton-service/internal/domain"
)

// recorder captures Apply call order instead of touching a real Store.
type recorder struct {
	mu    sync.Mutex
	order []domain.Event
}

func (r *recorder) Apply(ev domain.Event, _ time.Time) (bool, string) {
	r.mu.Lock()
	r.order = append(r.order, ev)
	r.mu.Unlock()
	return true, ""
}

func (r *recorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.order)
}

// TestPrioDrainsFirst prefills bulk, parks one fall in prio, then starts
// the worker. The fall must be applied before any bulk backlog.
func TestPrioDrainsFirst(t *testing.T) {
	r := &recorder{}
	prio := make(chan domain.Event, 10)
	bulk := make(chan domain.Event, 10)
	for i := 0; i < 5; i++ {
		bulk <- domain.Event{DeviceID: "d-bulk", RoomID: "r1", Type: "heartbeat"}
	}
	prio <- domain.Event{DeviceID: "d-fall", RoomID: "r1", Type: "fall_warn"}

	sh := &Shards{store: r, nowFunc: time.Now}
	go sh.loop(prio, bulk)

	deadline := time.Now().Add(2 * time.Second)
	for r.len() < 6 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := r.len(); got != 6 {
		t.Fatalf("applied=%d want 6", got)
	}
	r.mu.Lock()
	first := r.order[0]
	r.mu.Unlock()
	if first.Type != "fall_warn" {
		t.Fatalf("first applied=%q want fall_warn (prio jumped bulk)", first.Type)
	}
}

// TestEnqueueRoutesByType checks falls land in prio, rest in bulk.
func TestEnqueueRoutesByType(t *testing.T) {
	const n = 4
	sh := &Shards{
		prio: make([]chan domain.Event, n),
		bulk: make([]chan domain.Event, n),
	}
	for i := range sh.prio {
		sh.prio[i] = make(chan domain.Event, 10)
		sh.bulk[i] = make(chan domain.Event, 10)
	}
	fall := domain.Event{DeviceID: "d1", RoomID: "r1", Type: "fall_warn"}
	sh.Enqueue(fall)
	if got := len(sh.prio[shardOf("d1", n)]); got != 1 {
		t.Fatalf("prio len=%d want 1", got)
	}
	hb := domain.Event{DeviceID: "d1", RoomID: "r1", Type: "heartbeat"}
	sh.Enqueue(hb)
	if got := len(sh.bulk[shardOf("d1", n)]); got != 1 {
		t.Fatalf("bulk len=%d want 1", got)
	}
}
