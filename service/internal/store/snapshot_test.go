package store

import (
	"path/filepath"
	"testing"
	"time"

	"teton-service/internal/domain"
)

func mkEv(dev, room, typ string, ts time.Time) domain.Event {
	return domain.Event{DeviceID: dev, RoomID: room, Type: typ, Ts: ts, TsRaw: ts.Format(time.RFC3339Nano)}
}

func mkEvPresence(dev, room string, ts time.Time, in bool) domain.Event {
	e := mkEv(dev, room, "presence", ts)
	e.InRoom = &in
	return e
}

func mkEvFall(dev, room string, ts time.Time, raw string) domain.Event {
	return domain.Event{DeviceID: dev, RoomID: room, Type: "fall_warn", Ts: ts, TsRaw: raw}
}

func TestSnapshotRoundTrip(t *testing.T) {
	s := New()
	now := time.Now()
	ts := now.Add(-time.Minute)
	raw := ts.Format(time.RFC3339Nano)
	v := true
	s.Apply(mkEv("d1", "r1", "heartbeat", ts), now)
	s.Apply(mkEvPresence("d1", "r1", ts, v), now)
	s.Apply(mkEvFall("d1", "r1", ts, raw), now)
	path := filepath.Join(t.TempDir(), "latest.json")
	if err := s.Save(path, 42); err != nil {
		t.Fatal(err)
	}
	restored, off, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if off != 42 {
		t.Fatalf("off=%d want 42", off)
	}
	if got := len(restored.AlarmsSince(time.Time{})); got != 1 {
		t.Fatalf("alarms=%d want 1", got)
	}
	if _, avail := restored.Health("d1", now); avail <= 0 {
		t.Fatalf("health lost on restore")
	}
	inRoom, _ := restored.Occupancy("r1", time.Minute, now)
	if !inRoom {
		t.Fatalf("presence lost on restore")
	}
	// Dedup survives restore: same fall re-applied is still a dup.
	restored.Apply(mkEvFall("d1", "r1", ts, raw), now)
	if got := len(restored.AlarmsSince(time.Time{})); got != 1 {
		t.Fatalf("after dup alarms=%d want 1", got)
	}
}
