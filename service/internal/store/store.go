package store

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"teton-service/internal/domain"
)

const (
	healthWindow = 5 * time.Minute // ~1Hz -> 300 per 5m
	healthExpect = 300.0

	// Same fall = same device + same ts.
	maxPresencePerRoom = 5000
)

type presencePoint struct {
	ts     time.Time
	inRoom bool
}

type deviceHealth struct {
	last time.Time
	hb   []time.Time
}

// Store keeps queryable state behind a RWMutex.
type Store struct {
	mu       sync.RWMutex
	health   map[string]*deviceHealth
	presence map[string][]presencePoint
	alarms   []domain.Alarm
	seenFall map[string]struct{}

	latMu  sync.Mutex
	latMs  []float64
	emitMs []float64

	subsMu sync.Mutex
	subs   map[chan domain.Alarm]struct{}

	dropped atomic.Uint64
}

func New() *Store {
	return &Store{
		health:   make(map[string]*deviceHealth),
		presence: make(map[string][]presencePoint),
		seenFall: make(map[string]struct{}),
		subs:     make(map[chan domain.Alarm]struct{}),
	}
}

// Apply adds one event; callers pre-validate timestamps. False means rejected.
func (s *Store) Apply(ev domain.Event, now time.Time) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch ev.Type {
	case "heartbeat":
		h := s.health[ev.DeviceID]
		if h == nil {
			h = &deviceHealth{}
			s.health[ev.DeviceID] = h
		}
		if ev.Ts.After(h.last) {
			h.last = ev.Ts
		}
		h.hb = append(h.hb, ev.Ts)
		if len(h.hb)%64 == 0 {
			cut := now.Add(-healthWindow)
			i := sort.Search(len(h.hb), func(i int) bool { return !h.hb[i].Before(cut) })
			h.hb = append([]time.Time(nil), h.hb[i:]...)
		}

	case "presence":
		if ev.InRoom == nil {
			return false, "presence missing in_room"
		}
		pts := s.presence[ev.RoomID]
		pts = insertSorted(pts, presencePoint{ts: ev.Ts, inRoom: *ev.InRoom})
		if len(pts) > maxPresencePerRoom {
			pts = pts[len(pts)-maxPresencePerRoom:]
		}
		s.presence[ev.RoomID] = pts

	case "fall_warn":
		key := ev.DeviceID + "|" + ev.TsRaw
		if _, dup := s.seenFall[key]; dup {
			return false, "duplicate fall (same device+ts)"
		}
		s.seenFall[key] = struct{}{}
		conf := 0.0
		if ev.Conf != nil {
			conf = *ev.Conf
		}
		a := domain.Alarm{
			EventID:    fmt.Sprintf("%s@%s", ev.DeviceID, ev.Ts.UTC().Format(time.RFC3339Nano)),
			DeviceID:   ev.DeviceID,
			RoomID:     ev.RoomID,
			Ts:         ev.Ts,
			TsRaw:      ev.TsRaw,
			Confidence: conf,
			IngestTs:   ev.IngestTs,
		}
		s.alarms = insertAlarmSorted(s.alarms, a)
		s.broadcast(a)
		if !ev.IngestTs.IsZero() {
			lat := now.Sub(ev.IngestTs)
			if lat < 0 {
				lat = 0
			}
			s.latMu.Lock()
			s.latMs = append(s.latMs, float64(lat)/float64(time.Millisecond))
			s.latMu.Unlock()
		}
	}
	// motion / sleep_state / net_status are accepted as-is.
	return true, ""
}

// ObserveFallLatency records one ingest-to-broadcast delay.
func (s *Store) ObserveFallLatency(d time.Duration) {
	s.latMu.Lock()
	s.latMs = append(s.latMs, float64(d)/float64(time.Millisecond))
	s.latMu.Unlock()
}

// FallLatencySnapshot returns p50/p95 over observed fall delays in ms.
func (s *Store) FallLatencySnapshot() (p50, p95 float64, n int) {
	s.latMu.Lock()
	defer s.latMu.Unlock()
	n = len(s.latMs)
	if n == 0 {
		return 0, 0, 0
	}
	cp := append([]float64(nil), s.latMs...)
	sort.Float64s(cp)
	p50 = cp[int(0.50*float64(n-1))]
	p95 = cp[int(0.95*float64(n-1))]
	return p50, p95, n
}

// ObserveEmitLatency records one ingest-to-wire delay (post-Flush).
func (s *Store) ObserveEmitLatency(d time.Duration) {
	s.latMu.Lock()
	s.emitMs = append(s.emitMs, float64(d)/float64(time.Millisecond))
	s.latMu.Unlock()
}

// EmitLatencySnapshot returns p50/p95 over ingest-to-wire delays in ms.
func (s *Store) EmitLatencySnapshot() (p50, p95 float64, n int) {
	s.latMu.Lock()
	defer s.latMu.Unlock()
	n = len(s.emitMs)
	if n == 0 {
		return 0, 0, 0
	}
	cp := append([]float64(nil), s.emitMs...)
	sort.Float64s(cp)
	p50 = cp[int(0.50*float64(n-1))]
	p95 = cp[int(0.95*float64(n-1))]
	return p50, p95, n
}

func insertSorted(pts []presencePoint, p presencePoint) []presencePoint {
	i := sort.Search(len(pts), func(i int) bool { return !pts[i].ts.Before(p.ts) })
	pts = append(pts, presencePoint{})
	copy(pts[i+1:], pts[i:])
	pts[i] = p
	return pts
}

func insertAlarmSorted(as []domain.Alarm, a domain.Alarm) []domain.Alarm {
	i := sort.Search(len(as), func(i int) bool { return !as[i].Ts.Before(a.Ts) })
	as = append(as, domain.Alarm{})
	copy(as[i+1:], as[i:])
	as[i] = a
	return as
}

// Health returns the latest heartbeat and 5m availability in [0,1].
func (s *Store) Health(deviceID string, now time.Time) (*time.Time, float64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h := s.health[deviceID]
	if h == nil || h.last.IsZero() {
		return nil, 0
	}
	cut := now.Add(-healthWindow)
	n := 0
	for _, t := range h.hb {
		if !t.Before(cut) && !t.After(now.Add(30*time.Second)) {
			n++
		}
	}
	last := h.last
	return &last, min(float64(n)/healthExpect, 1.0)
}

// Occupancy reports current presence and share of window occupied.
func (s *Store) Occupancy(roomID string, window time.Duration, now time.Time) (bool, float64) {
	s.mu.RLock()
	pts := s.presence[roomID]
	s.mu.RUnlock()
	if len(pts) == 0 {
		return false, 0
	}
	latest := pts[len(pts)-1]
	start := now.Add(-window)

	// State at window start.
	state := false
	i := sort.Search(len(pts), func(i int) bool { return pts[i].ts.After(start) }) - 1
	if i >= 0 {
		state = pts[i].inRoom
	}
	cursor := start
	var occupied time.Duration
	for j := i + 1; j < len(pts); j++ {
		t := pts[j].ts
		if t.After(now) {
			break
		}
		if t.After(cursor) {
			if state {
				occupied += t.Sub(cursor)
			}
			cursor = t
		}
		state = pts[j].inRoom
	}
	if state && now.After(cursor) {
		occupied += now.Sub(cursor)
	}
	return latest.inRoom, min(float64(occupied)/float64(window), 1.0)
}

// AlarmsSince returns distinct falls with ts > since, oldest first.
func (s *Store) AlarmsSince(since time.Time) []domain.Alarm {
	s.mu.RLock()
	defer s.mu.RUnlock()
	i := sort.Search(len(s.alarms), func(i int) bool { return s.alarms[i].Ts.After(since) })
	out := make([]domain.Alarm, len(s.alarms)-i)
	copy(out, s.alarms[i:])
	return out
}

// AlarmsSinceInclusive returns distinct falls with ts >= since, oldest
// first. The SSE feed resumes with it so a reconnect redelivers the
// boundary alarm (clients dedup by event_id) instead of losing
// same-timestamp siblings that an exclusive bound would skip.
func (s *Store) AlarmsSinceInclusive(since time.Time) []domain.Alarm {
	s.mu.RLock()
	defer s.mu.RUnlock()
	i := sort.Search(len(s.alarms), func(i int) bool { return !s.alarms[i].Ts.Before(since) })
	out := make([]domain.Alarm, len(s.alarms)-i)
	copy(out, s.alarms[i:])
	return out
}

// Subscribe registers a live-feed listener.
func (s *Store) Subscribe(buf int) chan domain.Alarm {
	ch := make(chan domain.Alarm, buf)
	s.subsMu.Lock()
	s.subs[ch] = struct{}{}
	s.subsMu.Unlock()
	return ch
}

func (s *Store) Unsubscribe(ch chan domain.Alarm) {
	s.subsMu.Lock()
	delete(s.subs, ch)
	s.subsMu.Unlock()
}

func (s *Store) broadcast(a domain.Alarm) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for ch := range s.subs {
		select {
		case ch <- a:
		default:
			s.dropped.Add(1)
		}
	}
}

// DroppedDeliveries counts live alarms skipped because a subscriber buffer was full.
func (s *Store) DroppedDeliveries() uint64 {
	return s.dropped.Load()
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
