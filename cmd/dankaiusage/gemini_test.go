package main

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

const geminiQuotaFixture = `{"groups":[
 {"displayName":"Claude and GPT models","buckets":[
  {"bucketId":"3p-weekly","window":"weekly","resetTime":"2026-11-26T20:33:18Z","remainingFraction":1}]},
 {"displayName":"Gemini Models","buckets":[
  {"bucketId":"gemini-weekly","window":"weekly","resetTime":"2026-11-25T16:44:01Z","remainingFraction":0.25},
  {"bucketId":"gemini-5h","window":"5h","resetTime":"2026-11-19T23:21:39Z","remainingFraction":0.75}]}]}`

func TestParseGeminiQuotaSummaryUsesGeminiGroup(t *testing.T) {
	weekly, additional, err := parseGeminiQuotaSummary([]byte(geminiQuotaFixture))
	if err != nil {
		t.Fatal(err)
	}
	if !weekly.Known || weekly.PercentRemaining != 25 || weekly.ResetAt != "2026-11-25T16:44:01Z" || weekly.WindowMinutes != 7*24*60 {
		t.Fatalf("weekly = %+v", weekly)
	}
	if len(additional) != 1 {
		t.Fatalf("additional = %+v", additional)
	}
	five := additional[0]
	if five.ID != "gemini-5h" || five.Kind != "scoped" || five.Allowance.PercentRemaining != 75 || five.Allowance.WindowMinutes != 300 {
		t.Fatalf("5h bucket = %+v", five)
	}
}

func TestParseGeminiQuotaSummaryWithoutGeminiGroup(t *testing.T) {
	if _, _, err := parseGeminiQuotaSummary([]byte(`{"groups":[{"displayName":"Claude and GPT models","buckets":[]}]}`)); err == nil {
		t.Fatal("expected an error without a Gemini group")
	}
}

func TestCollectGeminiLimitsServesStaleCacheAfterFailure(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	path := filepath.Join(t.TempDir(), "gemini-usage.json")
	now := time.Date(2026, 11, 19, 20, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	ok := func(string) ([]byte, time.Duration, error) { return []byte(geminiQuotaFixture), 0, nil }
	if weekly, _, _, _, err := collectGeminiLimits(path, 5*time.Minute, clock, ok); err != nil || weekly.PercentRemaining != 25 {
		t.Fatalf("first fetch: %+v %v", weekly, err)
	}

	now = now.Add(6 * time.Minute)
	failing := func(string) ([]byte, time.Duration, error) {
		return nil, 5 * time.Minute, errors.New("Gemini quota API unreachable: offline")
	}
	weekly, _, meta, info, err := collectGeminiLimits(path, 5*time.Minute, clock, failing)
	if err != nil || weekly.PercentRemaining != 25 || !info.Stale || meta["source"] != geminiQuotaSource {
		t.Fatalf("stale fallback: %+v %+v %v", weekly, info, err)
	}
}
