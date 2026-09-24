package store

import (
	"testing"
	"time"

	"teton-service/internal/domain"
)

func ev(dev, room, typ string, ts time.Time) domain.Event {
	e := domain.Event{DeviceID: dev, RoomID: room, Type: typ, Ts: ts, TsRaw: ts.Format(time.RFC3339Nano)}
	if typ == "presence" {
		v := true
		e.InRoom = &v
	}
	return e
}

func TestFallDedup(t *testing.T) {
	s := New()
	now := time.Now()
	ts := now.Add(-time.Minute)
	tsRaw := ts.Format(time.RFC3339Nano)
	// Case A: jitter triple with identical ts -> 1 alarm.
	for i := 0; i < 3; i++ {
		s.Apply(domain.Event{DeviceID: "d1", RoomID: "r1", Type: "fall_warn", Ts: ts, TsRaw: tsRaw}, now)
	}
	if got := len(s.AlarmsSince(time.Time{})); got != 1 {
		t.Fatalf("want 1 distinct fall after jitter triple, got %d", got)
	}
	// Case B: genuine second fall 3s later (different ts) -> kept.
	ts2 := ts.Add(3 * time.Second)
	s.Apply(domain.Event{DeviceID: "d1", RoomID: "r1", Type: "fall_warn", Ts: ts2, TsRaw: ts2.Format(time.RFC3339Nano)}, now)
	if got := len(s.AlarmsSince(time.Time{})); got != 2 {
		t.Fatalf("want 2 distinct falls after second fall 3s later, got %d", got)
	}
	// Original ts preserved on the first alarm.
	if got := s.AlarmsSince(time.Time{})[0].TsRaw; got != tsRaw {
		t.Fatalf("first alarm ts=%v want original %v", got, tsRaw)
	}
}

func TestOccupancyLateReplay(t *testing.T) {
	s := New()
	now := time.Now()
	mk := func(ts time.Time, in bool) domain.Event {
		e := ev("d1", "r1", "presence", ts)
		e.InRoom = &in
		return e
	}
	// Room occupied for the last 30s, but the "enter" event arrives late.
	s.Apply(mk(now.Add(-10*time.Second), false), now) // leave (arrives first)
	if _, pct := s.Occupancy("r1", time.Minute, now); pct != 0 {
		t.Fatalf("before replay pct=%v want 0", pct)
	}
	s.Apply(mk(now.Add(-30*time.Second), true), now) // late enter
	_, pct := s.Occupancy("r1", time.Minute, now)
	// occupied 20s of 60s ~= 0.33
	if pct < 0.3 || pct > 0.37 {
		t.Fatalf("after replay pct=%v want ~0.33", pct)
	}
	// Latest by ts wins even though leave arrived first.
	inRoom, _ := s.Occupancy("r1", time.Minute, now)
	if inRoom {
		t.Fatalf("latest ts is leave, in_room must be false")
	}
}

func TestBroadcastDropCounter(t *testing.T) {
	s := New()
	if got := s.DroppedDeliveries(); got != 0 {
		t.Fatalf("fresh store dropped=%d want 0", got)
	}
	ch := s.Subscribe(1)
	defer s.Unsubscribe(ch)
	now := time.Now()
	// Buffer holds 1; the next 2 live alarms overflow it.
	for i := 0; i < 3; i++ {
		ts := now.Add(time.Duration(i) * time.Second)
		s.Apply(domain.Event{DeviceID: "d1", RoomID: "r1", Type: "fall_warn", Ts: ts, TsRaw: ts.Format(time.RFC3339Nano)}, now)
	}
	if got := s.DroppedDeliveries(); got != 2 {
		t.Fatalf("dropped=%d want 2", got)
	}
	// Draining does not change the count; all 3 alarms are still stored.
	<-ch
	if got := s.DroppedDeliveries(); got != 2 {
		t.Fatalf("after drain dropped=%d want 2", got)
	}
	if got := len(s.AlarmsSince(time.Time{})); got != 3 {
		t.Fatalf("stored alarms=%d want 3", got)
	}
}

func TestHealthUsesEventTime(t *testing.T) {
	s := New()
	now := time.Now()
	for i := 0; i < 300; i++ {
		s.Apply(ev("d1", "r1", "heartbeat", now.Add(-time.Duration(300-i)*time.Second)), now)
	}
	_, avail := s.Health("d1", now)
	if avail < 0.99 {
		t.Fatalf("avail=%v want ~1.0", avail)
	}
}
