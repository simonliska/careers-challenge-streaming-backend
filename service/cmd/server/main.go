package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"teton-service/internal/domain"
	"teton-service/internal/httpapi"
	"teton-service/internal/ingest"
	"teton-service/internal/store"
	"teton-service/internal/wal"
)

func main() {
	addr := getenv("ADDR", ":9090")
	dataDir := getenv("DATA_DIR", "data")
	snapPath := filepath.Join(dataDir, "snapshots", "latest.json")
	walPath := filepath.Join(dataDir, "wal.log")

	// Restore: snapshot first, then WAL.
	st, snapOff, err := store.LoadSnapshot(snapPath)
	if err != nil {
		log.Fatalf("load snapshot: %v", err)
	}
	w, err := wal.Open(walPath)
	if err != nil {
		log.Fatalf("open wal: %v", err)
	}

	total, err := wal.Replay(walPath, snapOff, func(ev domain.Event) error {
		st.Apply(ev, ev.Ts)
		return nil
	})
	if err != nil {
		log.Fatalf("replay wal: %v", err)
	}
	w.SetCount(total)
	log.Printf("restored snapshot_off=%d wal_total=%d alarms=%d", snapOff, total, len(st.AlarmsSince(time.Time{})))

	shards := ingest.New(st, 64, 2048)
	srv := &http.Server{
		Addr:         addr,
		Handler:      httpapi.New(st, shards, w).Routes(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 0, // SSE streams stay open; no write deadline
		IdleTimeout:  60 * time.Second,
	}

	// Periodic snapshot so replay stays short.
	stopSnap := make(chan struct{})
	go func() {
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if err := st.Save(snapPath, w.Count()); err != nil {
					log.Printf("snapshot: %v", err)
				}
			case <-stopSnap:
				return
			}
		}
	}()

	go func() {
		log.Printf("teton-service listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	close(stopSnap)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	if err := st.Save(snapPath, w.Count()); err != nil {
		log.Printf("final snapshot: %v", err)
	}
	if err := w.Close(); err != nil {
		log.Printf("wal close: %v", err)
	}
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
