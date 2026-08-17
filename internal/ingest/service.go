// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

inserted, err := s.store.InsertEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}
	if err := s.store.InsertEvent(ctx, rec); err != nil {
		return err
	}
	if err := s.store.UpsertCall(ctx, rec); err != nil {
		return err
	}
	if err := s.store.IncrementAccountStats(ctx, rec.AccountID, rec.DurationSec); err != nil {
		return err
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		// Create a detached background context so it doesn't get canceled when the HTTP request finishes.
		bgCtx := context.Background()
		go func() {
			if err := s.processRecording(bgCtx, rec); err != nil {
				s.log.Error("failed to process recording", "call_id", rec.CallID, "error", err)
			}
		}()
	}
	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
