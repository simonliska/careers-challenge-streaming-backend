package wal

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"teton-service/internal/domain"
)

const syncInterval = 10 * time.Millisecond

// Writer is an append-only JSON-lines log. Bulk records are written to the
// OS page cache and fsynced by the 10ms background ticker; fall_warn records
// fsync immediately so a crash cannot lose an alarm that was already acked.
type Writer struct {
	mu    sync.Mutex
	f     *os.File
	dirty bool
	count int64
	done  chan struct{}
}

// Open creates the parent dir, opens (or creates) the log for append, and
// starts the background fsync ticker.
func Open(path string) (*Writer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	w := &Writer{f: f, done: make(chan struct{})}
	go w.loop()
	return w, nil
}

func (w *Writer) loop() {
	t := time.NewTicker(syncInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			w.mu.Lock()
			if w.dirty {
				_ = w.f.Sync()
				w.dirty = false
			}
			w.mu.Unlock()
		case <-w.done:
			return
		}
	}
}

// Append writes one event and returns its 1-based offset. Falls fsync inline.
func (w *Writer) Append(ev domain.Event) (int64, error) {
	b, err := json.Marshal(ev)
	if err != nil {
		return 0, err
	}
	b = append(b, '\n')
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Write(b); err != nil {
		return 0, err
	}
	w.count++
	if ev.Type == "fall_warn" {
		if err := w.f.Sync(); err != nil {
			return 0, err
		}
		w.dirty = false
		return w.count, nil
	}
	w.dirty = true
	// Bulk fsyncs via ticker only; never inline.
	return w.count, nil
}

// Count reports records appended this run (plus restored offset via SetCount).
func (w *Writer) Count() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.count
}

// SetCount seeds the offset after replay (snapshot offset + replayed tail).
func (w *Writer) SetCount(n int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.count = n
}

// Sync flushes page cache to disk.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.dirty = false
	return nil
}

// Close stops the ticker, fsyncs, and closes the file.
func (w *Writer) Close() error {
	select {
	case <-w.done:
	default:
		close(w.done)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.f.Sync()
	return w.f.Close()
}

// Replay reads every JSON line, skipping the first skip records (covered by
// a snapshot), and calls fn for the rest in order.
func Replay(path string, skip int64, fn func(domain.Event) error) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	var off int64
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 128*1024), 1024*1024)
	for sc.Scan() {
		off++
		if off <= skip {
			continue
		}
		var ev domain.Event
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			continue // skip torn lines (kill -9 mid-write)
		}
		ts, ok := domain.ParseTime(ev.TsRaw)
		if !ok {
			continue
		}
		ev.Ts = ts
		if err := fn(ev); err != nil {
			return off, err
		}
	}
	return off, sc.Err()
}
