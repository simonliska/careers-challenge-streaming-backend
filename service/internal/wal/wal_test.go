package wal

import (
	"path/filepath"
	"testing"
	"time"

	"teton-service/internal/domain"
)

func TestAppendReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Now().Add(-time.Minute).Truncate(time.Millisecond).UTC()
	mk := func(typ, suffix string) domain.Event {
		raw := ts.Format(time.RFC3339Nano)
		return domain.Event{DeviceID: "d1" + suffix, RoomID: "r1", Type: typ, TsRaw: raw, Ts: ts}
	}
	evs := []domain.Event{mk("heartbeat", ""), mk("fall_warn", ""), mk("presence", "2")}
	for _, ev := range evs {
		if _, err := w.Append(ev); err != nil {
			t.Fatal(err)
		}
	}
	if got := w.Count(); got != 3 {
		t.Fatalf("count=%d want 3", got)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var got []domain.Event
	if _, err := Replay(path, 0, func(ev domain.Event) error {
		got = append(got, ev)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[1].Type != "fall_warn" {
		t.Fatalf("replayed %d events, want 3 with fall second", len(got))
	}
	// Skip (snapshot offset) resumes the tail only.
	var tail []domain.Event
	if _, err := Replay(path, 2, func(ev domain.Event) error {
		tail = append(tail, ev)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(tail) != 1 {
		t.Fatalf("tail=%d want 1", len(tail))
	}
}
