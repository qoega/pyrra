// backfill seeds a ClickHouse instance with sample SLO recording data
// so Pyrra has something to display in its UI.
//
// Usage:
//
//	go run ./examples/clickhouse/backfill \
//	  --address=<clickhouse-host>:8443 \
//	  --database=pyrra --username=default --password=<password> --tls
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func main() {
	addr := flag.String("address", "localhost:9000", "ClickHouse address (host:port)")
	database := flag.String("database", "pyrra", "ClickHouse database")
	username := flag.String("username", "default", "ClickHouse username")
	password := flag.String("password", "", "ClickHouse password")
	useTLS := flag.Bool("tls", false, "Enable TLS (for ClickHouse Cloud)")
	protocol := flag.String("protocol", "native", "Protocol: native or http")
	days := flag.Int("days", 28, "Number of days of data to backfill")
	stepMinutes := flag.Int("step", 5, "Step between data points in minutes")
	flag.Parse()

	ctx := context.Background()

	opts := &clickhouse.Options{
		Addr: []string{*addr},
		Auth: clickhouse.Auth{
			Database: *database,
			Username: *username,
			Password: *password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 30 * time.Second,
	}

	if *protocol == "http" {
		opts.Protocol = clickhouse.HTTP
	}
	if *useTLS {
		opts.TLS = &tls.Config{}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping: %v", err)
	}
	log.Println("Connected to ClickHouse")

	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-time.Duration(*days) * 24 * time.Hour)
	step := time.Duration(*stepMinutes) * time.Minute

	// Count total timestamps
	numTimestamps := 0
	for t := start; !t.After(now); t = t.Add(step) {
		numTimestamps++
	}
	log.Printf("Backfilling %d days of data (%d data points per series, step=%dm)", *days, numTimestamps, *stepMinutes)

	// ──────────────────────────────────────────────
	// SLO: http-errors (ratio, grouping by handler)
	// Metrics: http_requests:increase4w, http_requests:burnrate{5m,30m,1h,2h,6h,1d,4d}
	// ──────────────────────────────────────────────
	handlers := []string{"/api/v1/query", "/api/v1/write", "/api/v1/labels", "/healthz"}
	httpErrorsWindows := []string{"5m", "30m", "1h", "2h", "6h", "1d", "4d"}

	log.Println("Seeding http-errors SLO recordings...")
	seedRatioSLO(ctx, conn, "http-errors", "http_requests", "job", "api-server",
		"handler", handlers, httpErrorsWindows, start, now, step, 0.005) // ~0.5% error rate

	// ──────────────────────────────────────────────
	// SLO: http-latency (latency, grouping by handler)
	// Metrics: http_request_duration_seconds:increase4w, http_request_duration_seconds:burnrate{...}
	// ──────────────────────────────────────────────
	log.Println("Seeding http-latency SLO recordings...")
	seedLatencySLO(ctx, conn, "http-latency", "http_request_duration_seconds", "job", "api-server",
		"handler", handlers, httpErrorsWindows, start, now, step, 0.02) // ~2% slow requests

	// ──────────────────────────────────────────────
	// SLO: probe-success (bool_gauge, grouping by instance)
	// Metrics: probe_success:count4w, probe_success:sum4w, probe_success:burnrate{...}
	// ──────────────────────────────────────────────
	instances := []string{"https://example.com", "https://api.example.com", "https://status.example.com"}

	log.Println("Seeding probe-success SLO recordings...")
	seedBoolGaugeSLO(ctx, conn, "probe-success", "probe_success", "job", "blackbox",
		"instance", instances, httpErrorsWindows, start, now, step, 0.001) // ~0.1% failure rate

	log.Println("Backfill complete!")
}

// seedRatioSLO seeds increase and burnrate recordings for a ratio SLO
func seedRatioSLO(
	ctx context.Context, conn driver.Conn,
	sloName, baseMetric, labelKey, labelVal, groupKey string,
	groupValues, windows []string,
	start, end time.Time, step time.Duration,
	errorRate float64,
) {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO slo_recordings (timestamp, metric_name, labels, value)")
	if err != nil {
		log.Fatalf("prepare batch: %v", err)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	count := 0

	for _, handler := range groupValues {
		labels := map[string]string{
			labelKey: labelVal,
			groupKey: handler,
			"slo":    sloName,
		}

		// increase4w - cumulative total over 4w window
		baseTotal := 100000.0 + rng.Float64()*50000
		for t := start; !t.After(end); t = t.Add(step) {
			// Gradually increase total over time
			elapsed := t.Sub(start).Hours()
			totalDays := end.Sub(start).Hours()
			progress := elapsed / totalDays
			total := baseTotal * (0.5 + 0.5*progress) * (1 + 0.05*math.Sin(elapsed/24*2*math.Pi))

			batch.Append(t, fmt.Sprintf("%s:increase4w", baseMetric), labels, total)
			count++

			// error_increase4w - cumulative error total over 4w window.
			// In ClickHouse, this is a distinct metric name from the total increase
			// (unlike Prometheus where both share the same recording rule name).
			errors := total * errorRate
			batch.Append(t, fmt.Sprintf("%s:error_increase4w", baseMetric), labels, errors)
			count++
		}

		// burnrate for each window
		for _, w := range windows {
			for t := start; !t.After(end); t = t.Add(step) {
				// Burnrate = actual error rate / budget
				// Add some realistic variation
				elapsed := t.Sub(start).Hours()
				noise := 0.3 * math.Sin(elapsed/6*2*math.Pi) // 6h cycle
				spike := 0.0
				// Add occasional spikes
				if rng.Float64() < 0.02 {
					spike = rng.Float64() * 3.0
				}
				burnrate := errorRate * 100 * (1 + noise + spike)
				if burnrate < 0 {
					burnrate = 0.001
				}

				batch.Append(t, fmt.Sprintf("%s:burnrate%s", baseMetric, w), labels, burnrate)
				count++
			}
		}
	}

	if err := batch.Send(); err != nil {
		log.Fatalf("send batch: %v", err)
	}
	log.Printf("  Inserted %d rows for %s", count, sloName)
}

// seedLatencySLO seeds increase and burnrate recordings for a latency SLO
func seedLatencySLO(
	ctx context.Context, conn driver.Conn,
	sloName, baseMetric, labelKey, labelVal, groupKey string,
	groupValues, windows []string,
	start, end time.Time, step time.Duration,
	slowRate float64,
) {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO slo_recordings (timestamp, metric_name, labels, value)")
	if err != nil {
		log.Fatalf("prepare batch: %v", err)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	count := 0

	for _, handler := range groupValues {
		labels := map[string]string{
			labelKey: labelVal,
			groupKey: handler,
			"slo":    sloName,
		}

		// increase4w - total requests over the window
		baseTotal := 80000.0 + rng.Float64()*40000
		for t := start; !t.After(end); t = t.Add(step) {
			elapsed := t.Sub(start).Hours()
			totalDays := end.Sub(start).Hours()
			progress := elapsed / totalDays
			total := baseTotal * (0.5 + 0.5*progress)

			batch.Append(t, fmt.Sprintf("%s:increase4w", baseMetric), labels, total)
			count++

			// success_increase4w - cumulative successful (fast) requests over 4w window.
			// In ClickHouse, this is a distinct metric name from the total increase
			// (unlike Prometheus where both share the same recording rule name).
			success := total * (1 - slowRate)
			batch.Append(t, fmt.Sprintf("%s:success_increase4w", baseMetric), labels, success)
			count++
		}

		// burnrate for each window
		for _, w := range windows {
			for t := start; !t.After(end); t = t.Add(step) {
				elapsed := t.Sub(start).Hours()
				noise := 0.2 * math.Sin(elapsed/12*2*math.Pi)
				spike := 0.0
				if rng.Float64() < 0.015 {
					spike = rng.Float64() * 5.0
				}
				burnrate := slowRate * (100.0 / 5.0) * (1 + noise + spike) // target=95%
				if burnrate < 0 {
					burnrate = 0.001
				}

				batch.Append(t, fmt.Sprintf("%s:burnrate%s", baseMetric, w), labels, burnrate)
				count++
			}
		}
	}

	if err := batch.Send(); err != nil {
		log.Fatalf("send batch: %v", err)
	}
	log.Printf("  Inserted %d rows for %s", count, sloName)
}

// seedBoolGaugeSLO seeds count, sum, and burnrate recordings for a bool gauge SLO
func seedBoolGaugeSLO(
	ctx context.Context, conn driver.Conn,
	sloName, metricName, labelKey, labelVal, groupKey string,
	groupValues, windows []string,
	start, end time.Time, step time.Duration,
	failRate float64,
) {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO slo_recordings (timestamp, metric_name, labels, value)")
	if err != nil {
		log.Fatalf("prepare batch: %v", err)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	count := 0

	for _, instance := range groupValues {
		labels := map[string]string{
			labelKey: labelVal,
			groupKey: instance,
			"slo":    sloName,
		}

		// count4w - total probe attempts
		baseCount := 50000.0 + rng.Float64()*20000
		for t := start; !t.After(end); t = t.Add(step) {
			elapsed := t.Sub(start).Hours()
			totalDays := end.Sub(start).Hours()
			progress := elapsed / totalDays
			total := baseCount * (0.5 + 0.5*progress)

			batch.Append(t, fmt.Sprintf("%s:count4w", metricName), labels, total)
			count++
		}

		// sum4w - successful probes
		for t := start; !t.After(end); t = t.Add(step) {
			elapsed := t.Sub(start).Hours()
			totalDays := end.Sub(start).Hours()
			progress := elapsed / totalDays
			total := baseCount * (0.5 + 0.5*progress)
			successes := total * (1 - failRate)

			batch.Append(t, fmt.Sprintf("%s:sum4w", metricName), labels, successes)
			count++
		}

		// burnrate for each window
		for _, w := range windows {
			for t := start; !t.After(end); t = t.Add(step) {
				elapsed := t.Sub(start).Hours()
				noise := 0.2 * math.Sin(elapsed/8*2*math.Pi)
				spike := 0.0
				if rng.Float64() < 0.01 {
					spike = rng.Float64() * 10.0
				}
				// target=99.9%, budget=0.1%
				burnrate := failRate * (100.0 / 0.1) * (1 + noise + spike)
				if burnrate < 0 {
					burnrate = 0.001
				}

				batch.Append(t, fmt.Sprintf("%s:burnrate%s", metricName, w), labels, burnrate)
				count++
			}
		}
	}

	if err := batch.Send(); err != nil {
		log.Fatalf("send batch: %v", err)
	}
	log.Printf("  Inserted %d rows for %s", count, sloName)
}
