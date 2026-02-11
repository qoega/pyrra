package clickhouse

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

// TimeSeries represents a single time series with its samples.
type TimeSeries struct {
	MetricName string
	Labels     map[string]string
	MetricType string // "counter", "gauge", "histogram", "unknown"
	Samples    []Sample
}

// Sample is a single (timestamp, value) pair.
type Sample struct {
	Timestamp time.Time
	Value     float64
}

// MetricGenerator produces synthetic time series data for testing.
type MetricGenerator struct {
	rng *rand.Rand
}

// NewMetricGenerator creates a new generator with deterministic seed for reproducibility.
func NewMetricGenerator(seed int64) *MetricGenerator {
	return &MetricGenerator{rng: rand.New(rand.NewSource(seed))}
}

// GenerateCounter produces a monotonically increasing counter time series.
// ratePerSecond is the average increment per second, with optional jitter.
func (g *MetricGenerator) GenerateCounter(
	metricName string,
	labelSets []map[string]string,
	start, end time.Time,
	step time.Duration,
	ratePerSecond float64,
	jitter float64,
) []TimeSeries {
	var series []TimeSeries
	for _, ls := range labelSets {
		var samples []Sample
		value := 0.0
		for ts := start; !ts.After(end); ts = ts.Add(step) {
			increment := ratePerSecond * step.Seconds()
			if jitter > 0 {
				increment += g.rng.Float64() * jitter * increment
			}
			value += increment
			samples = append(samples, Sample{Timestamp: ts, Value: value})
		}
		series = append(series, TimeSeries{
			MetricName: metricName,
			Labels:     ls,
			MetricType: "counter",
			Samples:    samples,
		})
	}
	return series
}

// GenerateGauge produces a gauge time series with random walk behavior.
func (g *MetricGenerator) GenerateGauge(
	metricName string,
	labelSets []map[string]string,
	start, end time.Time,
	step time.Duration,
	baseValue float64,
	variance float64,
) []TimeSeries {
	var series []TimeSeries
	for _, ls := range labelSets {
		var samples []Sample
		value := baseValue
		for ts := start; !ts.After(end); ts = ts.Add(step) {
			value += (g.rng.Float64()*2 - 1) * variance
			if value < 0 {
				value = 0
			}
			samples = append(samples, Sample{Timestamp: ts, Value: value})
		}
		series = append(series, TimeSeries{
			MetricName: metricName,
			Labels:     ls,
			MetricType: "gauge",
			Samples:    samples,
		})
	}
	return series
}

// GenerateBoolGauge produces a boolean gauge (0 or 1) time series.
// failureRate is the probability of a 0 sample (failure).
func (g *MetricGenerator) GenerateBoolGauge(
	metricName string,
	labelSets []map[string]string,
	start, end time.Time,
	step time.Duration,
	failureRate float64,
) []TimeSeries {
	var series []TimeSeries
	for _, ls := range labelSets {
		var samples []Sample
		for ts := start; !ts.After(end); ts = ts.Add(step) {
			value := 1.0
			if g.rng.Float64() < failureRate {
				value = 0.0
			}
			samples = append(samples, Sample{Timestamp: ts, Value: value})
		}
		series = append(series, TimeSeries{
			MetricName: metricName,
			Labels:     ls,
			MetricType: "gauge",
			Samples:    samples,
		})
	}
	return series
}

// GenerateHistogram produces histogram bucket time series for a latency metric.
// It generates _bucket, _count, and _sum series.
func (g *MetricGenerator) GenerateHistogram(
	metricName string,
	labelSets []map[string]string,
	start, end time.Time,
	step time.Duration,
	meanLatency float64,
	buckets []float64,
	requestsPerStep float64,
) []TimeSeries {
	var allSeries []TimeSeries

	for _, ls := range labelSets {
		bucketSeries := make(map[float64]*TimeSeries)
		for _, b := range buckets {
			bl := copyLabels(ls)
			bl["le"] = fmt.Sprintf("%g", b)
			bucketSeries[b] = &TimeSeries{
				MetricName: metricName + "_bucket",
				Labels:     bl,
				MetricType: "histogram",
			}
		}
		// +Inf bucket
		infLabels := copyLabels(ls)
		infLabels["le"] = "+Inf"
		bucketSeries[math.Inf(1)] = &TimeSeries{
			MetricName: metricName + "_bucket",
			Labels:     infLabels,
			MetricType: "histogram",
		}

		countSeries := &TimeSeries{
			MetricName: metricName + "_count",
			Labels:     copyLabels(ls),
			MetricType: "histogram",
		}
		sumSeries := &TimeSeries{
			MetricName: metricName + "_sum",
			Labels:     copyLabels(ls),
			MetricType: "histogram",
		}

		cumulativeBuckets := make(map[float64]float64)
		for _, b := range buckets {
			cumulativeBuckets[b] = 0
		}
		cumulativeBuckets[math.Inf(1)] = 0
		var totalCount, totalSum float64

		for ts := start; !ts.After(end); ts = ts.Add(step) {
			// Generate N requests with log-normal latency distribution
			n := requestsPerStep + g.rng.Float64()*requestsPerStep*0.2
			for i := 0.0; i < n; i++ {
				latency := g.rng.ExpFloat64() * meanLatency
				totalSum += latency
				totalCount++
				for _, b := range buckets {
					if latency <= b {
						cumulativeBuckets[b]++
					}
				}
				cumulativeBuckets[math.Inf(1)]++
			}

			for _, b := range buckets {
				bucketSeries[b].Samples = append(bucketSeries[b].Samples, Sample{Timestamp: ts, Value: cumulativeBuckets[b]})
			}
			bucketSeries[math.Inf(1)].Samples = append(bucketSeries[math.Inf(1)].Samples, Sample{Timestamp: ts, Value: cumulativeBuckets[math.Inf(1)]})
			countSeries.Samples = append(countSeries.Samples, Sample{Timestamp: ts, Value: totalCount})
			sumSeries.Samples = append(sumSeries.Samples, Sample{Timestamp: ts, Value: totalSum})
		}

		for _, b := range buckets {
			allSeries = append(allSeries, *bucketSeries[b])
		}
		allSeries = append(allSeries, *bucketSeries[math.Inf(1)])
		allSeries = append(allSeries, *countSeries, *sumSeries)
	}
	return allSeries
}

func copyLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// --- Predefined test scenarios ---

// HTTPRatioScenario generates http_requests_total with success and error responses.
// Returns series for total requests and error requests matching a Ratio SLO.
// errorRate controls what fraction of requests return 5xx codes.
func HTTPRatioScenario(start, end time.Time, step time.Duration, errorRate float64) []TimeSeries {
	gen := NewMetricGenerator(42)

	successLabels := []map[string]string{
		{"job": "api", "handler": "/api", "code": "200"},
	}
	errorLabels := []map[string]string{
		{"job": "api", "handler": "/api", "code": "500"},
	}

	baseRate := 100.0 // 100 requests/sec total

	// Success requests: (1-errorRate) fraction
	successSeries := gen.GenerateCounter(
		"http_requests_total",
		successLabels,
		start, end, step,
		baseRate*(1-errorRate),
		0.05,
	)

	// Error requests: errorRate fraction
	errorSeries := gen.GenerateCounter(
		"http_requests_total",
		errorLabels,
		start, end, step,
		baseRate*errorRate,
		0.05,
	)

	return append(successSeries, errorSeries...)
}

// LatencyScenario generates http_request_duration_seconds histogram data.
func LatencyScenario(start, end time.Time, step time.Duration) []TimeSeries {
	gen := NewMetricGenerator(42)

	labelSets := []map[string]string{
		{"job": "api", "handler": "/api"},
	}

	buckets := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0}

	return gen.GenerateHistogram(
		"http_request_duration_seconds",
		labelSets,
		start, end, step,
		0.1,    // mean latency: 100ms
		buckets,
		10,     // ~10 requests per step
	)
}

// BoolGaugeScenario generates probe success/failure data.
func BoolGaugeScenario(start, end time.Time, step time.Duration, failureRate float64) []TimeSeries {
	gen := NewMetricGenerator(42)

	labelSets := []map[string]string{
		{"job": "blackbox", "instance": "example.com"},
	}

	return gen.GenerateBoolGauge(
		"probe_success",
		labelSets,
		start, end, step,
		failureRate,
	)
}

// BurstErrorScenario generates normal traffic followed by a burst of errors.
// This simulates an error budget burn event.
func BurstErrorScenario(start, end time.Time, step time.Duration, burstStart time.Time, burstErrorRate float64) []TimeSeries {
	gen := NewMetricGenerator(42)

	var successSamples, errorSamples []Sample
	successValue := 0.0
	errorValue := 0.0
	normalErrorRate := 0.001 // 0.1% normal
	baseRate := 100.0

	for ts := start; !ts.After(end); ts = ts.Add(step) {
		currentErrorRate := normalErrorRate
		if !ts.Before(burstStart) {
			currentErrorRate = burstErrorRate
		}

		successInc := baseRate * (1 - currentErrorRate) * step.Seconds()
		errorInc := baseRate * currentErrorRate * step.Seconds()

		successInc += gen.rng.Float64() * 0.05 * successInc
		errorInc += gen.rng.Float64() * 0.05 * errorInc

		successValue += successInc
		errorValue += errorInc

		successSamples = append(successSamples, Sample{Timestamp: ts, Value: successValue})
		errorSamples = append(errorSamples, Sample{Timestamp: ts, Value: errorValue})
	}

	return []TimeSeries{
		{
			MetricName: "http_requests_total",
			Labels:     map[string]string{"job": "api", "handler": "/api", "code": "200"},
			MetricType: "counter",
			Samples:    successSamples,
		},
		{
			MetricName: "http_requests_total",
			Labels:     map[string]string{"job": "api", "handler": "/api", "code": "500"},
			MetricType: "counter",
			Samples:    errorSamples,
		},
	}
}

// --- Backfill utilities ---

// BackfillClickHouse inserts time series data into the metrics_raw table using batch insert.
func BackfillClickHouse(t *testing.T, client *Client, series []TimeSeries) {
	t.Helper()

	ctx := context.Background()

	batch, err := client.PrepareBatch(ctx, "INSERT INTO metrics_raw (timestamp, metric_name, labels, value, metric_type)")
	if err != nil {
		t.Fatalf("failed to prepare batch: %v", err)
	}

	for _, ts := range series {
		for _, sample := range ts.Samples {
			err := batch.Append(
				sample.Timestamp,
				ts.MetricName,
				ts.Labels,
				sample.Value,
				metricTypeOrDefault(ts.MetricType),
			)
			if err != nil {
				t.Fatalf("failed to append sample for %s: %v", ts.MetricName, err)
			}
		}
	}

	if err := batch.Send(); err != nil {
		t.Fatalf("failed to send batch: %v", err)
	}
}

// BackfillClickHouseBatch is a faster version that batches inserts with configurable batch size.
func BackfillClickHouseBatch(t *testing.T, client *Client, series []TimeSeries) {
	t.Helper()

	ctx := context.Background()
	const batchSize = 10000

	type row struct {
		ts         time.Time
		metricName string
		labels     map[string]string
		value      float64
		metricType string
	}

	var rows []row
	for _, ts := range series {
		for _, sample := range ts.Samples {
			rows = append(rows, row{
				ts:         sample.Timestamp,
				metricName: ts.MetricName,
				labels:     ts.Labels,
				value:      sample.Value,
				metricType: metricTypeOrDefault(ts.MetricType),
			})
		}
	}

	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[i:end]

		batch, err := client.PrepareBatch(ctx, "INSERT INTO metrics_raw (timestamp, metric_name, labels, value, metric_type)")
		if err != nil {
			t.Fatalf("failed to prepare batch: %v", err)
		}

		for _, r := range chunk {
			if err := batch.Append(r.ts, r.metricName, r.labels, r.value, r.metricType); err != nil {
				t.Fatalf("failed to append row: %v", err)
			}
		}

		if err := batch.Send(); err != nil {
			t.Fatalf("failed to send batch: %v", err)
		}
	}
}

// insertRecordings inserts data into the slo_recordings table using batch insert.
func insertRecordings(t *testing.T, client *Client, rows []recordingRow) {
	t.Helper()

	ctx := context.Background()

	batch, err := client.PrepareBatch(ctx, "INSERT INTO slo_recordings (timestamp, metric_name, labels, value)")
	if err != nil {
		t.Fatalf("failed to prepare slo_recordings batch: %v", err)
	}

	for _, r := range rows {
		if err := batch.Append(r.Timestamp, r.MetricName, r.Labels, r.Value); err != nil {
			t.Fatalf("failed to append recording row: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		t.Fatalf("failed to send slo_recordings batch: %v", err)
	}
}

// recordingRow represents a single row in the slo_recordings table.
type recordingRow struct {
	Timestamp  time.Time
	MetricName string
	Labels     map[string]string
	Value      float64
}

func metricTypeOrDefault(mt string) string {
	switch mt {
	case "counter", "gauge", "histogram", "summary":
		return mt
	default:
		return "unknown"
	}
}

// TotalSamples returns the total number of samples across all series.
func TotalSamples(series []TimeSeries) int {
	total := 0
	for _, ts := range series {
		total += len(ts.Samples)
	}
	return total
}
