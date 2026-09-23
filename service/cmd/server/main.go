package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"teton-service/internal/httpapi"
	"teton-service/internal/ingest"
	"teton-service/internal/store"
)

func main() {
	addr := getenv("ADDR", ":9090")
	st := store.New()
	shards := ingest.New(st, 64, 2048)
	srv := &http.Server{
		Addr:         addr,
		Handler:      httpapi.New(st, shards).Routes(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 0, // SSE streams stay open; no write deadline
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("teton-service listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
