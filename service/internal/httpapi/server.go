package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"teton-service/internal/domain"
	"teton-service/internal/ingest"
	"teton-service/internal/store"
	"teton-service/internal/wal"
)

const maxBody = 64 * 1024

type Server struct {
	st     *store.Store
	shards *ingest.Shards
	wal    *wal.Writer
	now    func() time.Time
}

// New wires the store and shard workers into the HTTP handlers.
func New(st *store.Store, sh *ingest.Shards, w *wal.Writer) *Server {
	return &Server{st: st, shards: sh, wal: w, now: time.Now}
}

// Routes exposes the ingest and query endpoints.
func (s *Server) Routes() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("POST /events", s.postEvents)
	m.HandleFunc("GET /devices/{device_id}/health", s.getHealth)
	m.HandleFunc("GET /rooms/{room_id}/occupancy", s.getOccupancy)
	m.HandleFunc("GET /alarms", s.getAlarms)
	m.HandleFunc("GET /alarms/feed", s.getFeed)
	m.HandleFunc("GET /metrics", s.getMetrics)
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]bool{"ok": true})
	})
	return m
}

func (s *Server) postEvents(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	var raw struct {
		DeviceID string   `json:"device_id"`
		RoomID   string   `json:"room_id"`
		Type     string   `json:"type"`
		Ts       string   `json:"ts"`
		Seq      int64    `json:"seq"`
		InRoom   *bool    `json:"in_room"`
		Conf     *float64 `json:"confidence"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}
	if raw.DeviceID == "" || raw.RoomID == "" || !domain.ValidType(raw.Type) || raw.Ts == "" {
		writeJSON(w, 400, map[string]string{"error": "missing device_id/room_id/type/ts"})
		return
	}
	ts, ok := domain.ParseTime(raw.Ts)
	if !ok {
		writeJSON(w, 400, map[string]string{"error": "unparseable ts"})
		return
	}
	now := s.now()
	if ts.After(now.Add(time.Hour)) {
		writeJSON(w, 400, map[string]string{"error": "ts more than 1h in future"})
		return
	}
	// Offline buffers replay up to 1h past per spec.
	if ts.Before(now.Add(-time.Hour)) {
		writeJSON(w, 400, map[string]string{"error": "ts more than 1h in past"})
		return
	}
	if raw.Type == "presence" && raw.InRoom == nil {
		writeJSON(w, 400, map[string]string{"error": "presence missing in_room"})
		return
	}
	ev := domain.Event{
		DeviceID: raw.DeviceID, RoomID: raw.RoomID, Type: raw.Type,
		TsRaw: raw.Ts, Seq: raw.Seq, InRoom: raw.InRoom, Conf: raw.Conf, Ts: ts,
		IngestTs: now,
	}
	// Append-before-ack: WAL first, then in-memory. Falls fsync inline.
	if s.wal != nil {
		if _, err := s.wal.Append(ev); err != nil {
			writeJSON(w, 500, map[string]string{"error": "wal append failed"})
			return
		}
	}
	s.shards.Enqueue(ev)
	writeJSON(w, 202, map[string]bool{"ok": true})
}

func (s *Server) getMetrics(w http.ResponseWriter, _ *http.Request) {
	p50, p95, n := s.st.FallLatencySnapshot()
	ep50, ep95, en := s.st.EmitLatencySnapshot()
	var walTotal int64
	if s.wal != nil {
		walTotal = s.wal.Count()
	}
	writeJSON(w, 200, map[string]any{
		"fall_latency_ms":      map[string]any{"p50": p50, "p95": p95, "count": n},
		"fall_emit_latency_ms": map[string]any{"p50": ep50, "p95": ep95, "count": en},
		"wal_total":            walTotal,
		"alarms_total":         len(s.st.AlarmsSince(time.Time{})),
	})
}

func (s *Server) getHealth(w http.ResponseWriter, r *http.Request) {
	last, avail := s.st.Health(r.PathValue("device_id"), s.now())
	var raw *string
	if last != nil {
		v := last.UTC().Format(time.RFC3339Nano)
		raw = &v
	}
	writeJSON(w, 200, map[string]any{"last_heartbeat_ts": raw, "availability_5m": avail})
}

func (s *Server) getOccupancy(w http.ResponseWriter, r *http.Request) {
	win, ok := parseWindow(r.URL.Query().Get("window"))
	if !ok {
		writeJSON(w, 400, map[string]string{"error": "window must be 1m|5m|1h"})
		return
	}
	inRoom, pct := s.st.Occupancy(r.PathValue("room_id"), win, s.now())
	writeJSON(w, 200, map[string]any{
		"in_room": inRoom, "occupied_pct": pct, "window_seconds": int(win.Seconds()),
	})
}

func (s *Server) getAlarms(w http.ResponseWriter, r *http.Request) {
	since, ok := domain.ParseTime(r.URL.Query().Get("since"))
	if !ok {
		writeJSON(w, 400, map[string]string{"error": "bad since"})
		return
	}
	alarms := s.st.AlarmsSince(since)
	items := make([]map[string]any, 0, len(alarms))
	for _, a := range alarms {
		items = append(items, map[string]any{
			"event_id": a.EventID, "device_id": a.DeviceID,
			"room_id": a.RoomID, "ts": a.TsRaw, "confidence": a.Confidence,
		})
	}
	writeJSON(w, 200, map[string]any{"alarms": items})
}

// getFeed replays alarms at since, then streams new falls as SSE.
// Replay is inclusive so resume redelivers the boundary alarm (dedup by
// event_id) instead of dropping same-timestamp siblings. Live items are
// all post-subscribe by construction; replayed IDs are skipped once to
// cover the subscribe/replay overlap without a lossy timestamp watermark.
func (s *Server) getFeed(w http.ResponseWriter, r *http.Request) {
	since, ok := domain.ParseTime(r.URL.Query().Get("since"))
	if !ok {
		writeJSON(w, 400, map[string]string{"error": "bad since"})
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, 500, map[string]string{"error": "sse unsupported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := s.st.Subscribe(128)
	defer s.st.Unsubscribe(ch)

	replayed := s.st.AlarmsSinceInclusive(since)
	sent := make(map[string]struct{}, len(replayed))
	for _, a := range replayed {
		fmt.Fprintf(w, "data: %s\n\n", alarmJSON(a))
		sent[a.EventID] = struct{}{}
	}
	fl.Flush()

	keep := time.NewTicker(15 * time.Second)
	defer keep.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case a := <-ch:
			if _, dup := sent[a.EventID]; dup {
				delete(sent, a.EventID)
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", alarmJSON(a))
			fl.Flush()
			if !a.IngestTs.IsZero() {
				lat := s.now().Sub(a.IngestTs)
				if lat < 0 {
					lat = 0
				}
				s.st.ObserveEmitLatency(lat)
			}
		case <-keep.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			fl.Flush()
		}
	}
}

func alarmJSON(a domain.Alarm) string {
	b, _ := json.Marshal(map[string]any{
		"event_id": a.EventID, "device_id": a.DeviceID,
		"room_id": a.RoomID, "ts": a.TsRaw, "confidence": a.Confidence,
	})
	return string(b)
}

func parseWindow(s string) (time.Duration, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "5m":
		return 5 * time.Minute, true
	case "1m":
		return time.Minute, true
	case "1h":
		return time.Hour, true
	}
	return 0, false
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
