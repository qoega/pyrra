package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/pyrra-dev/pyrra/slo"
)

// TimestampedValue holds a single data point from slo_recordings.
type TimestampedValue struct {
	Timestamp time.Time
	Value     float64
}

// LabeledValue holds a value with its associated labels.
type LabeledValue struct {
	Labels map[string]string
	Value  float64
}

// LabeledTimeseries holds a full timeseries with labels.
type LabeledTimeseries struct {
	Labels map[string]string
	Values []TimestampedValue
}

// escapeSQLString escapes single quotes in SQL strings to prevent injection.
// Uses SQL-standard doubling of single quotes.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// labelFingerprint computes a stable fingerprint from a label map, used for grouping.
func labelFingerprint(ls map[string]string) model.Fingerprint {
	m := make(model.Metric, len(ls))
	for k, v := range ls {
		m[model.LabelName(k)] = model.LabelValue(v)
	}
	return m.Fingerprint()
}

// BuildLabelConditions builds SQL WHERE conditions from label matchers.
// It returns a SQL fragment like: AND labels['key'] = 'value'
func BuildLabelConditions(groupingMatchers []*labels.Matcher) string {
	var conditions []string
	for _, m := range groupingMatchers {
		if m.Name == labels.MetricName {
			continue
		}
		switch m.Type {
		case labels.MatchEqual:
			conditions = append(conditions, fmt.Sprintf("AND labels['%s'] = '%s'", escapeSQLString(m.Name), escapeSQLString(m.Value)))
		case labels.MatchNotEqual:
			conditions = append(conditions, fmt.Sprintf("AND labels['%s'] != '%s'", escapeSQLString(m.Name), escapeSQLString(m.Value)))
		case labels.MatchRegexp:
			conditions = append(conditions, fmt.Sprintf("AND match(labels['%s'], '%s')", escapeSQLString(m.Name), escapeSQLString(m.Value)))
		case labels.MatchNotRegexp:
			conditions = append(conditions, fmt.Sprintf("AND NOT match(labels['%s'], '%s')", escapeSQLString(m.Name), escapeSQLString(m.Value)))
		}
	}
	return strings.Join(conditions, "\n  ")
}

// QueryLatestRecordings fetches the latest values for a given metric_name and slo label
// from slo_recordings using FINAL for ReplacingMergeTree deduplication.
// Returns a map from label fingerprint to LabeledValue.
func QueryLatestRecordings(
	ctx context.Context,
	client *Client,
	logger log.Logger,
	metricName string,
	sloName string,
	extraConditions string,
) (map[model.Fingerprint]LabeledValue, error) {
	query := fmt.Sprintf(`
SELECT labels, value
FROM slo_recordings FINAL
WHERE metric_name = '%s'
  AND labels['slo'] = '%s'
  %s
  AND timestamp = (
    SELECT max(timestamp) FROM slo_recordings FINAL
    WHERE metric_name = '%s'
      AND labels['slo'] = '%s'
      %s
  )`,
		escapeSQLString(metricName),
		escapeSQLString(sloName),
		extraConditions,
		escapeSQLString(metricName),
		escapeSQLString(sloName),
		extraConditions,
	)

	level.Debug(logger).Log("msg", "queryLatestRecordings", "query", query)

	rows, err := client.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("queryLatestRecordings: %w", err)
	}
	defer rows.Close()

	results := make(map[model.Fingerprint]LabeledValue)
	for rows.Next() {
		var (
			labelsMap map[string]string
			value     float64
		)
		if err := rows.Scan(&labelsMap, &value); err != nil {
			return nil, fmt.Errorf("scan latest recording: %w", err)
		}

		fp := labelFingerprint(labelsMap)
		results[fp] = LabeledValue{
			Labels: labelsMap,
			Value:  value,
		}
	}

	return results, nil
}

// QueryRecordingRange fetches a time series range for a metric from slo_recordings FINAL.
// Returns a map from label fingerprint to LabeledTimeseries.
func QueryRecordingRange(
	ctx context.Context,
	client *Client,
	logger log.Logger,
	metricName string,
	sloName string,
	extraConditions string,
	start, end time.Time,
) (map[model.Fingerprint]*LabeledTimeseries, error) {
	query := fmt.Sprintf(`
SELECT timestamp, labels, value
FROM slo_recordings FINAL
WHERE metric_name = '%s'
  AND labels['slo'] = '%s'
  %s
  AND timestamp >= toDateTime('%s')
  AND timestamp <= toDateTime('%s')
ORDER BY timestamp`,
		escapeSQLString(metricName),
		escapeSQLString(sloName),
		extraConditions,
		start.UTC().Format("2006-01-02 15:04:05"),
		end.UTC().Format("2006-01-02 15:04:05"),
	)

	level.Debug(logger).Log("msg", "queryRecordingRange", "query", query)

	rows, err := client.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("queryRecordingRange: %w", err)
	}
	defer rows.Close()

	results := make(map[model.Fingerprint]*LabeledTimeseries)
	for rows.Next() {
		var (
			ts        time.Time
			labelsMap map[string]string
			value     float64
		)
		if err := rows.Scan(&ts, &labelsMap, &value); err != nil {
			return nil, fmt.Errorf("scan recording range: %w", err)
		}

		fp := labelFingerprint(labelsMap)
		if _, exists := results[fp]; !exists {
			results[fp] = &LabeledTimeseries{
				Labels: labelsMap,
			}
		}
		results[fp].Values = append(results[fp].Values, TimestampedValue{
			Timestamp: ts,
			Value:     value,
		})
	}

	return results, nil
}

// QueryBurnrateLatest fetches the latest burnrate values for all windows of an objective.
// Returns a map from window duration to map of fingerprint to LabeledValue.
func QueryBurnrateLatest(
	ctx context.Context,
	client *Client,
	logger log.Logger,
	objective slo.Objective,
	extraConditions string,
) (map[time.Duration]map[model.Fingerprint]LabeledValue, error) {
	sloName := objective.Name()
	result := make(map[time.Duration]map[model.Fingerprint]LabeledValue)

	// Collect all unique windows
	windowSet := make(map[time.Duration]struct{})
	for _, w := range objective.Windows() {
		windowSet[w.Short] = struct{}{}
		windowSet[w.Long] = struct{}{}
	}

	for window := range windowSet {
		metricName := objective.BurnrateName(window)
		values, err := QueryLatestRecordings(ctx, client, logger, metricName, sloName, extraConditions)
		if err != nil {
			level.Warn(logger).Log("msg", "failed to query burnrate", "window", window, "err", err)
			continue
		}
		result[window] = values
	}

	return result, nil
}

// ComputeErrorBudget computes the error budget remaining from total and error timeseries.
// Formula: ((1-target) - (errors/total)) / (1-target)
// Returns the result as a set of LabeledTimeseries keyed by fingerprint.
func ComputeErrorBudget(
	totalSeries map[model.Fingerprint]*LabeledTimeseries,
	errorSeries map[model.Fingerprint]*LabeledTimeseries,
	target float64,
) map[model.Fingerprint]*LabeledTimeseries {
	result := make(map[model.Fingerprint]*LabeledTimeseries)

	for fp, total := range totalSeries {
		errors, ok := errorSeries[fp]
		if !ok {
			continue
		}

		budget := &LabeledTimeseries{
			Labels: total.Labels,
		}

		// Build a quick lookup of error values by timestamp
		errorByTS := make(map[int64]float64, len(errors.Values))
		for _, ev := range errors.Values {
			errorByTS[ev.Timestamp.Unix()] = ev.Value
		}

		for _, tv := range total.Values {
			errorVal := errorByTS[tv.Timestamp.Unix()]
			totalVal := tv.Value

			var budgetVal float64
			if totalVal == 0 {
				budgetVal = 1.0 // No requests = full budget
			} else {
				errorRatio := errorVal / totalVal
				budgetVal = ((1 - target) - errorRatio) / (1 - target)
			}

			budget.Values = append(budget.Values, TimestampedValue{
				Timestamp: tv.Timestamp,
				Value:     budgetVal,
			})
		}

		result[fp] = budget
	}

	return result
}

// TotalMetricName returns the total/count metric name for the objective's window.
// Delegates to slo.Objective.TotalName() as the single source of truth.
func TotalMetricName(objective slo.Objective) string {
	return objective.TotalName()
}

// ErrorMetricName returns the error metric name for the objective's window in ClickHouse.
//
// For Ratio indicators, Prometheus uses the same recording rule name for both total and
// error increases (e.g., "http_requests:increase4w"), distinguishing them by label values.
// In ClickHouse, we use distinct metric names to avoid ambiguity:
//   - Total: "http_requests:increase4w"      (from Total.Name)
//   - Error: "http_errors:error_increase4w"   (from Errors.Name)
//
// For Latency/BoolGauge, errors are computed as (total - success), so this returns "".
func ErrorMetricName(objective slo.Objective) string {
	switch objective.IndicatorType() {
	case slo.Ratio:
		metric := objective.Indicator.Ratio.Errors.Name
		metric = strings.TrimSuffix(metric, "_total")
		metric = strings.TrimSuffix(metric, "_count")
		return fmt.Sprintf("%s:error_increase%s", metric, objective.Window)
	default:
		return ""
	}
}

// SuccessMetricName returns the success metric name for the objective's window in ClickHouse.
//
// For Latency indicators, Prometheus uses the same recording rule name for both total and
// success increases (both strip _count/_bucket to the same base), distinguishing by labels.
// In ClickHouse, we use distinct metric names:
//   - Total: "http_request_duration_seconds:increase4w"
//   - Success: "http_request_duration_seconds:success_increase4w"
//
// For BoolGauge, the names are already distinct (count vs sum), so we delegate to
// slo.Objective.SuccessName().
func SuccessMetricName(objective slo.Objective) string {
	switch objective.IndicatorType() {
	case slo.Latency:
		metric := objective.Indicator.Latency.Success.Name
		metric = strings.TrimSuffix(metric, "_total")
		metric = strings.TrimSuffix(metric, "_count")
		metric = strings.TrimSuffix(metric, "_bucket")
		return fmt.Sprintf("%s:success_increase%s", metric, objective.Window)
	case slo.BoolGauge:
		return objective.SuccessName()
	default:
		return ""
	}
}
