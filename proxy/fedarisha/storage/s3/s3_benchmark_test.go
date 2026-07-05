package s3

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path"
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
