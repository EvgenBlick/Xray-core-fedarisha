package s3

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type benchXrayConfig struct {
	Outbounds []struct {
		Protocol string `json:"protocol"`
		Settings struct {
			Storage struct {
				Type        string `json:"type"`
				Bucket      string `json:"bucket"`
				Prefix      string `json:"prefix"`
				Region      string `json:"region"`
				Endpoint    string `json:"endpoint"`
				AccessKey   string `json:"accessKey"`
				SecretKey   string `json:"secretKey"`
				SessionsDir string `json:"sessionsDir"`
			} `json:"storage"`
		} `json:"settings"`
	} `json:"outbounds"`
}

func TestRawS3BenchmarkFromXrayConfig(t *testing.T) {
	configPath := os.Getenv("FEDARISHA_S3_BENCH_CONFIG")
	if configPath == "" {
		t.Skip("set FEDARISHA_S3_BENCH_CONFIG to an Xray config with fedarisha S3 storage")
	}

	size := 32 * 1024 * 1024
	if v := os.Getenv("FEDARISHA_S3_BENCH_SIZE_MB"); v != "" {
		var mb int
		if _, err := fmt.Sscanf(v, "%d", &mb); err != nil || mb <= 0 {
			t.Fatalf("invalid FEDARISHA_S3_BENCH_SIZE_MB=%q", v)
		}
		size = mb * 1024 * 1024
	}

	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var cfg benchXrayConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}

	var s3cfg Config
	for _, outbound := range cfg.Outbounds {
		if outbound.Protocol != "fedarisha" {
			continue
		}
		st := outbound.Settings.Storage
		s3cfg = Config{
			Bucket:    st.Bucket,
			Prefix:    st.Prefix,
			Region:    st.Region,
			Endpoint:  st.Endpoint,
			AccessKey: st.AccessKey,
			SecretKey: st.SecretKey,
		}
		break
	}
	if s3cfg.Bucket == "" {
		t.Fatal("no fedarisha S3 storage found in config")
	}

	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}

	store := New(s3cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}

	key := path.Join("bench", fmt.Sprintf("raw-%d.bin", time.Now().UnixNano()))
	start := time.Now()
	if err := store.Upload(ctx, key, payload); err != nil {
		t.Fatal(err)
	}
	uploadDuration := time.Since(start)

	start = time.Now()
	got, err := store.Download(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	downloadDuration := time.Since(start)

	if len(got) != len(payload) {
		t.Fatalf("downloaded %d bytes, want %d", len(got), len(payload))
	}
	_ = store.Delete(context.Background(), key)

	mb := float64(size) / 1024 / 1024
	t.Logf("raw S3 object size: %.1f MiB", mb)
	t.Logf("raw S3 upload: %.2f MiB/s, %.2f Mbps, duration %v", mb/uploadDuration.Seconds(), mb*8/uploadDuration.Seconds(), uploadDuration)
	t.Logf("raw S3 download: %.2f MiB/s, %.2f Mbps, duration %v", mb/downloadDuration.Seconds(), mb*8/downloadDuration.Seconds(), downloadDuration)
}

func TestRawS3ParallelBenchmarkFromXrayConfig(t *testing.T) {
	configPath := os.Getenv("FEDARISHA_S3_BENCH_CONFIG")
	if configPath == "" {
		t.Skip("set FEDARISHA_S3_BENCH_CONFIG to an Xray config with fedarisha S3 storage")
	}
	workers := envInt(t, "FEDARISHA_S3_BENCH_WORKERS", 4)
	sizeMB := envInt(t, "FEDARISHA_S3_BENCH_SIZE_MB", 8)
	if workers <= 0 || sizeMB <= 0 {
		t.Fatal("workers and size must be positive")
	}

	store := New(loadBenchConfig(t, configPath))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, sizeMB*1024*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}

	paths := make([]string, workers)
	var uploadBytes atomic.Int64
	start := time.Now()
	runParallel(t, workers, func(i int) {
		key := path.Join("bench", fmt.Sprintf("parallel-%d-%d.bin", time.Now().UnixNano(), i))
		if err := store.Upload(ctx, key, payload); err != nil {
			t.Errorf("upload worker %d: %v", i, err)
			return
		}
		paths[i] = key
		uploadBytes.Add(int64(len(payload)))
	})
	uploadDuration := time.Since(start)

	var downloadBytes atomic.Int64
	start = time.Now()
	runParallel(t, workers, func(i int) {
		data, err := store.Download(ctx, paths[i])
		if err != nil {
			t.Errorf("download worker %d: %v", i, err)
			return
		}
		downloadBytes.Add(int64(len(data)))
	})
	downloadDuration := time.Since(start)

	for _, p := range paths {
		if p != "" {
			_ = store.Delete(context.Background(), p)
		}
	}

	logThroughput(t, "parallel raw S3 upload", uploadBytes.Load(), uploadDuration, workers)
	logThroughput(t, "parallel raw S3 download", downloadBytes.Load(), downloadDuration, workers)
}

func loadBenchConfig(t *testing.T, configPath string) Config {
	t.Helper()
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var cfg benchXrayConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	for _, outbound := range cfg.Outbounds {
		if outbound.Protocol != "fedarisha" {
			continue
		}
		st := outbound.Settings.Storage
		return Config{
			Bucket:    st.Bucket,
			Prefix:    st.Prefix,
			Region:    st.Region,
			Endpoint:  st.Endpoint,
			AccessKey: st.AccessKey,
			SecretKey: st.SecretKey,
		}
	}
	t.Fatal("no fedarisha S3 storage found in config")
	return Config{}
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		t.Fatalf("invalid %s=%q", name, v)
	}
	return n
}

func runParallel(t *testing.T, workers int, fn func(int)) {
	t.Helper()
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			fn(i)
		}(i)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
}

func logThroughput(t *testing.T, label string, bytes int64, d time.Duration, workers int) {
	t.Helper()
	mb := float64(bytes) / 1024 / 1024
	t.Logf("%s: workers=%d, %.1f MiB, %.2f MiB/s, %.2f Mbps, duration %v",
		label, workers, mb, mb/d.Seconds(), mb*8/d.Seconds(), d)
}
