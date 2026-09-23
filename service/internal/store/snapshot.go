package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"teton-service/internal/domain"
)

type healthDump struct {
	Last time.Time   `json:"last"`
	Hb   []time.Time `json:"hb"`
}

type presenceDump struct {
	Ts     time.Time `json:"ts"`
	InRoom bool      `json:"in_room"`
}

// Snapshot is the on-disk in json
type snapshot struct {
	WALOffset int64                     `json:"wal_offset"`
	Health    map[string]healthDump     `json:"health"`
	Presence  map[string][]presenceDump `json:"presence"`
	Alarms    []domain.Alarm            `json:"alarms"`
	SeenFall  []string                  `json:"seen_fall"`
}

// Save writes an atomic snapshot (tmp + rename) covering walOffset.
func (s *Store) Save(path string, walOffset int64) error {
	s.mu.RLock()
	snap := snapshot{
		WALOffset: walOffset,
		Health:    make(map[string]healthDump, len(s.health)),
		Presence:  make(map[string][]presenceDump, len(s.presence)),
		Alarms:    append([]domain.Alarm(nil), s.alarms...),
		SeenFall:  make([]string, 0, len(s.seenFall)),
	}
	for id, h := range s.health {
		snap.Health[id] = healthDump{Last: h.last, Hb: append([]time.Time(nil), h.hb...)}
	}
	for room, pts := range s.presence {
		out := make([]presenceDump, len(pts))
		for i, p := range pts {
			out[i] = presenceDump{Ts: p.ts, InRoom: p.inRoom}
		}
		snap.Presence[room] = out
	}
	for k := range s.seenFall {
		snap.SeenFall = append(snap.SeenFall, k)
	}
	s.mu.RUnlock()
	sort.Strings(snap.SeenFall)
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadSnapshot reads path into a ready Store. Missing file => fresh Store, 0.
func LoadSnapshot(path string) (*Store, int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return New(), 0, nil
		}
		return nil, 0, err
	}
	var snap snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return nil, 0, err
	}
	s := New()
	for id, h := range snap.Health {
		s.health[id] = &deviceHealth{last: h.Last, hb: append([]time.Time(nil), h.Hb...)}
	}
	for room, pts := range snap.Presence {
		out := make([]presencePoint, len(pts))
		for i, p := range pts {
			out[i] = presencePoint{ts: p.Ts, inRoom: p.InRoom}
		}
		s.presence[room] = out
	}
	for _, a := range snap.Alarms {
		if ts, ok := domain.ParseTime(a.TsRaw); ok {
			a.Ts = ts
		}
		s.alarms = append(s.alarms, a)
	}
	for _, k := range snap.SeenFall {
		s.seenFall[k] = struct{}{}
	}
	return s, snap.WALOffset, nil
}
