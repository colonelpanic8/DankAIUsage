package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	// agy's own backend; the production host answers with an untouched quota.
	geminiQuotaURL             = "https://daily-cloudcode-pa.googleapis.com/v1internal:retrieveUserQuotaSummary"
	geminiQuotaSource          = "antigravity quota api"
	geminiFallbackAgyVersion   = "1.5.5"
	geminiTokenRefreshTimeout  = 90 * time.Second
	geminiQuotaGroupNamePrefix = "gemini"
)

type geminiUsageCache struct {
	FetchedAt                 string          `json:"fetchedAt,omitempty"`
	Body                      json.RawMessage `json:"body,omitempty"`
	NextAttemptAt             string          `json:"nextAttemptAt,omitempty"`
	LastError                 string          `json:"lastError,omitempty"`
	DiagnosticCategory        string          `json:"diagnosticCategory,omitempty"`
	DiagnosticHTTPStatus      int             `json:"diagnosticHttpStatus,omitempty"`
	DiagnosticCooldownSeconds int64           `json:"diagnosticCooldownSeconds,omitempty"`
	AgyVersion                string          `json:"agyVersion,omitempty"`
}

type geminiQuotaSummary struct {
	Groups []struct {
		DisplayName string `json:"displayName"`
		Buckets     []struct {
			BucketID          string   `json:"bucketId"`
			DisplayName       string   `json:"displayName"`
			Window            string   `json:"window"`
			ResetTime         string   `json:"resetTime"`
			RemainingFraction *float64 `json:"remainingFraction"`
		} `json:"buckets"`
	} `json:"groups"`
}

func collectGemini(now time.Time, opts options) ProviderUsage {
	provider := ProviderUsage{
		ID:        "gemini",
		Name:      "Gemini",
		Available: hasCommand("agy"),
		CLIPath:   commandPath("agy"),
		DataPath:  filepath.Join(geminiHome(), "antigravity-cli"),
	}
	setProviderMeta(&provider, "limitDataSource", "Antigravity quota API")
	setProviderMeta(&provider, "tokenDataAvailable", false)
	setProviderMeta(&provider, "tokenDataNote", "Token history is not tracked for Gemini")
	provider.historyObservedAt = time.Now()
	weekly, additional, meta, info, err := collectGeminiLimitsWithClock(opts.RefreshInterval, time.Now)
	provider.WeeklyLeft = weekly
	provider.QuotaBuckets = makeQuotaBuckets(weekly, nil, additional)
	if meta == nil {
		meta = map[string]any{}
	}
	mergeUsageRefreshMeta(meta, info)
	mergeProviderMeta(&provider, meta)
	if !info.FetchedAt.IsZero() {
		provider.historyObservedAt = info.FetchedAt
	}
	if err != nil {
		setProviderMeta(&provider, "limitError", err.Error())
	}
	return provider
}

func geminiUsageCachePath() string {
	return filepath.Join(pluginStateDir(), "gemini-usage.json")
}

func loadGeminiUsageCache(path string) (geminiUsageCache, error) {
	var cache geminiUsageCache
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cache, nil
	}
	if err != nil {
		return cache, err
	}
	if err := json.Unmarshal(data, &cache); err != nil {
		return geminiUsageCache{}, fmt.Errorf("%w: Gemini cache unreadable", errUsageRefreshState)
	}
	return cache, nil
}

func saveGeminiUsageCache(path string, cache geminiUsageCache) error {
	data, err := json.Marshal(cache)
	if err != nil {
		return err
	}
	return atomicWriteUsageRefreshFile(path, data)
}

func collectGeminiLimitsWithClock(interval time.Duration, clock usageRefreshClock) (Allowance, []QuotaBucket, map[string]any, usageRefreshInfo, error) {
	return collectGeminiLimits(geminiUsageCachePath(), interval, clock, fetchGeminiQuotaSummary)
}

func collectGeminiLimits(path string, interval time.Duration, clock usageRefreshClock, fetch func(agyVersion string) ([]byte, time.Duration, error)) (Allowance, []QuotaBucket, map[string]any, usageRefreshInfo, error) {
	interval = normalizeUsageRefreshInterval(interval)
	var weekly Allowance
	var additional []QuotaBucket
	var meta map[string]any
	var info usageRefreshInfo
	var actionErr error
	var recovered bool
	fromCache := func(cache geminiUsageCache, cached, stale bool) error {
		var err error
		weekly, additional, err = parseGeminiQuotaSummary(cache.Body)
		if err != nil {
			return fmt.Errorf("%w: Gemini cache response unsupported", errUsageRefreshState)
		}
		meta = geminiUsageMeta(path)
		info.Cached, info.Stale = cached, stale
		return nil
	}
	err := withUsageRefreshLock(path, func() error {
		now := clock()
		cache, err := loadGeminiUsageCache(path)
		if err != nil {
			return err
		}
		fetchedAt, err := parseOptionalRefreshTime(cache.FetchedAt)
		if err != nil {
			return err
		}
		nextAttemptAt, err := parseOptionalRefreshTime(cache.NextAttemptAt)
		if err != nil {
			return err
		}
		if !fetchedAt.IsZero() && fetchedAt.Add(interval).After(nextAttemptAt) {
			nextAttemptAt = fetchedAt.Add(interval)
		}
		if fetchedAt.After(now) {
			return fmt.Errorf("%w: %w: Gemini cache future timestamp", errUsageRefreshState, errUsageRefreshTimestamp)
		}
		info = usageRefreshInfo{
			FetchedAt: fetchedAt, NextAttemptAt: nextAttemptAt, LastError: cache.LastError,
			DiagnosticCategory: cache.DiagnosticCategory, DiagnosticHTTPStatus: cache.DiagnosticHTTPStatus,
			DiagnosticCooldownSeconds: cache.DiagnosticCooldownSeconds,
		}
		cacheUsable := len(cache.Body) > 0 && !fetchedAt.IsZero()
		staleOK := cacheUsable && now.Sub(fetchedAt) < max(30*time.Minute, 2*interval)
		if now.Before(nextAttemptAt) {
			if cacheUsable && (cache.LastError == "" || staleOK) {
				return fromCache(cache, true, cache.LastError != "")
			}
			actionErr = errUsageRefreshCoolingDown
			return nil
		}

		cache.NextAttemptAt = now.Add(interval).UTC().Format(time.RFC3339Nano)
		if err := saveGeminiUsageCache(path, cache); err != nil {
			return errors.New("could not reserve Gemini usage refresh")
		}

		cache.AgyVersion = agyVersionString(cache.AgyVersion)
		body, backoff, fetchErr := fetch(cache.AgyVersion)
		if fetchErr == nil {
			if _, _, parseErr := parseGeminiQuotaSummary(body); parseErr != nil {
				fetchErr, backoff = parseErr, 5*time.Minute
			}
		}
		if fetchErr != nil {
			retryAt := now.Add(max(interval, backoff))
			cache.LastError = fetchErr.Error()
			cache.DiagnosticCategory, cache.DiagnosticHTTPStatus = classifyDiagnosticError("gemini", fetchErr)
			cache.DiagnosticCooldownSeconds = min(int64(max(interval, backoff)/time.Second), int64(diagnosticMaxCooldown/time.Second))
			cache.NextAttemptAt = retryAt.UTC().Format(time.RFC3339Nano)
			info.NextAttemptAt, info.LastError = retryAt, cache.LastError
			info.DiagnosticCategory, info.DiagnosticHTTPStatus, info.DiagnosticCooldownSeconds = cache.DiagnosticCategory, cache.DiagnosticHTTPStatus, cache.DiagnosticCooldownSeconds
			if err := saveGeminiUsageCache(path, cache); err != nil {
				return errors.New("could not save Gemini usage refresh failure")
			}
			if staleOK {
				return fromCache(cache, true, true)
			}
			actionErr = fetchErr
			return nil
		}

		recovered = cache.LastError != "" || cache.DiagnosticCategory != ""
		cache.Body = body
		cache.FetchedAt = now.UTC().Format(time.RFC3339Nano)
		cache.NextAttemptAt = now.Add(interval).UTC().Format(time.RFC3339Nano)
		cache.LastError, cache.DiagnosticCategory, cache.DiagnosticHTTPStatus, cache.DiagnosticCooldownSeconds = "", "", 0, 0
		if err := saveGeminiUsageCache(path, cache); err != nil {
			return errors.New("could not save refreshed Gemini usage")
		}
		info = usageRefreshInfo{FetchedAt: now, NextAttemptAt: now.Add(interval)}
		return fromCache(cache, false, false)
	})
	if err != nil {
		emitUsageRefreshDiagnostics("gemini", info, err, nil, false)
		return makeUnknownAllowance("weekly", clock()), nil, nil, usageRefreshInfo{}, err
	}
	if actionErr != nil {
		weekly = makeUnknownAllowance("weekly", clock())
		if errors.Is(actionErr, errUsageRefreshCoolingDown) {
			meta = usageRefreshMeta(info)
		}
	}
	emitUsageRefreshDiagnostics("gemini", info, nil, actionErr, recovered)
	return weekly, additional, meta, info, actionErr
}

func geminiUsageMeta(path string) map[string]any {
	return map[string]any{
		"source":          geminiQuotaSource,
		"limitDataSource": "Antigravity quota API",
		"usageCache":      path,
	}
}

// parseGeminiQuotaSummary maps the "Gemini Models" group: its weekly bucket
// is the provider's weekly quota and its other windows follow as scoped
// buckets. Other groups (Claude and GPT through Antigravity) are left out.
func parseGeminiQuotaSummary(body []byte) (Allowance, []QuotaBucket, error) {
	var summary geminiQuotaSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		return Allowance{}, nil, errors.New("Gemini quota API returned an unsupported response")
	}
	for _, group := range summary.Groups {
		if !strings.HasPrefix(strings.ToLower(group.DisplayName), geminiQuotaGroupNamePrefix) {
			continue
		}
		var weekly Allowance
		var additional []QuotaBucket
		for _, bucket := range group.Buckets {
			if bucket.RemainingFraction == nil {
				continue
			}
			resetAt, _ := time.Parse(time.RFC3339, bucket.ResetTime)
			window, minutes, label := geminiWindow(bucket.Window)
			allowance := makeSubscriptionAllowance(window, geminiQuotaSource, 100-*bucket.RemainingFraction*100, resetAt, minutes)
			if window == "weekly" && !weekly.Known {
				weekly = allowance
				continue
			}
			additional = append(additional, QuotaBucket{
				ID:        firstNonEmpty(bucket.BucketID, "gemini-"+bucket.Window),
				Label:     label,
				Kind:      "scoped",
				Allowance: allowance,
			})
		}
		if !weekly.Known && len(additional) == 0 {
			break
		}
		return weekly, additional, nil
	}
	return Allowance{}, nil, errors.New("Gemini quota API returned no Gemini quota")
}

func geminiWindow(window string) (string, int64, string) {
	switch window {
	case "weekly":
		return "weekly", 7 * 24 * 60, "Weekly"
	case "daily":
		return "daily", 24 * 60, "Daily"
	}
	if d, err := time.ParseDuration(window); err == nil && d > 0 {
		return window, int64(d / time.Minute), window
	}
	return window, 0, firstNonEmpty(window, "Quota")
}

func fetchGeminiQuotaSummary(agyVersion string) ([]byte, time.Duration, error) {
	token, err := antigravityAccessToken(time.Now())
	if err != nil {
		return nil, 5 * time.Minute, err
	}
	req, err := http.NewRequest(http.MethodPost, geminiQuotaURL, bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, 5 * time.Minute, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	// The Gemini group is reported only to the Antigravity CLI's identity.
	req.Header.Set("User-Agent", "antigravity/cli/"+agyVersion)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return nil, 2 * time.Minute, fmt.Errorf("Gemini quota API unreachable: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, 2 * time.Minute, errors.New("Gemini quota API response could not be read")
	}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, 15 * time.Minute, errors.New("Gemini quota API rate limited (HTTP 429)")
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, 5 * time.Minute, fmt.Errorf("Gemini quota API auth failed (HTTP %d); sign in with agy", resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return nil, 5 * time.Minute, fmt.Errorf("Gemini quota API returned HTTP %d", resp.StatusCode)
	}
	return body, 0, nil
}

// antigravityAccessToken reads the Antigravity CLI's sign-in from the Secret
// Service. An expired token is refreshed by agy itself (any agy command that
// talks to the backend does it), so the helper never handles refresh tokens.
func antigravityAccessToken(now time.Time) (string, error) {
	token, expiry, err := readAntigravityToken()
	if err != nil {
		return "", err
	}
	if expiry.IsZero() || expiry.After(now.Add(time.Minute)) {
		return token, nil
	}
	if !hasCommand("agy") {
		return "", errors.New("Antigravity OAuth token expired and agy is not on PATH")
	}
	ctx, cancel := context.WithTimeout(context.Background(), geminiTokenRefreshTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "agy", "models")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, io.Discard, io.Discard
	_ = cmd.Run()
	token, expiry, err = readAntigravityToken()
	if err != nil {
		return "", err
	}
	if !expiry.IsZero() && !expiry.After(now) {
		return "", errors.New("Antigravity OAuth token expired; run agy to refresh sign-in")
	}
	return token, nil
}

func readAntigravityToken() (string, time.Time, error) {
	if !hasCommand("secret-tool") {
		return "", time.Time{}, errors.New("Antigravity credentials unavailable: secret-tool is not on PATH")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "secret-tool", "lookup", "service", "gemini", "username", "antigravity").Output()
	if err != nil || len(bytes.TrimSpace(out)) == 0 {
		return "", time.Time{}, errors.New("Antigravity credentials not found; sign in with agy once")
	}
	var stored struct {
		Token struct {
			AccessToken string `json:"access_token"`
			Expiry      string `json:"expiry"`
		} `json:"token"`
	}
	if err := json.Unmarshal(out, &stored); err != nil || stored.Token.AccessToken == "" {
		return "", time.Time{}, errors.New("Antigravity credentials have no OAuth token")
	}
	expiry, _ := time.Parse(time.RFC3339Nano, stored.Token.Expiry)
	return stored.Token.AccessToken, expiry, nil
}

func agyVersionString(cached string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "agy", "--version").Output(); err == nil {
		if fields := strings.Fields(string(out)); len(fields) > 0 {
			return fields[len(fields)-1]
		}
	}
	return firstNonEmpty(cached, geminiFallbackAgyVersion)
}

func geminiHome() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".gemini")
	}
	return ".gemini"
}
