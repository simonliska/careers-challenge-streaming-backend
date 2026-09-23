package store

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"teton-service/internal/domain"
)

const (
	healthWindow = 5 * time.Minute // ~1Hz -> 300 per 5m
	healthExpect = 300.0

	// Same fall = same device + same ts.
	futureLimit = time.Hour
	pastLimit   = time.Hour

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

	subsMu sync.Mutex
	subs   map[chan domain.Alarm]struct{}
}

func New() *Store {
	return &Store{
		health:   make(map[string]*deviceHealth),
		presence: make(map[string][]presencePoint),
		seenFall: make(map[string]struct{}),
		subs:     make(map[chan domain.Alarm]struct{}),
	}
}

// Apply adds one event. False means rejected.
func (s *Store) Apply(ev domain.Event, now time.Time) (bool, string) {
	if ev.Ts.After(now.Add(futureLimit)) {
		return false, "ts more than 1h in future"
	}
	if ev.Ts.Before(now.Add(-pastLimit)) {
		return false, "ts more than 1h in past"
	}

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
		}
		s.alarms = insertAlarmSorted(s.alarms, a)
		s.broadcast(a)
	}
	// motion / sleep_state / net_status are accepted as-is.
	return true, ""
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
		}
	}
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
