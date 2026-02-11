package clickhouse

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/pyrra-dev/pyrra/slo"
	"github.com/stretchr/testify/require"
)

func TestSanitizeMVName(t *testing.T) {
	testcases := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"with-dash", "with_dash"},
		{"with.dot", "with_dot"},
		{"with/slash", "with_slash"},
		{"with space", "with_space"},
		{"complex-name.test/example", "complex_name_test_example"},
	}

	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			result := sanitizeMVName(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractBaseMetric(t *testing.T) {
	testcases := []struct {
		input    string
		expected string
	}{
		{"http_requests_total", "http_requests"},
		{"http_request_duration_bucket", "http_request_duration"},
		{"http_request_duration_count", "http_request_duration"},
		{"http_request_duration_sum", "http_request_duration"},
		{"simple_metric", "simple_metric"},
		{"metric_with_total_suffix_total", "metric_with_total_suffix"},
	}

	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			result := extractBaseMetric(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestFormatWindowDuration(t *testing.T) {
	testcases := []struct {
		duration time.Duration
		expected string
	}{
		{5 * time.Minute, "5m"},
		{30 * time.Minute, "30m"},
		{time.Hour, "1h"},
		{2 * time.Hour, "2h"},
		{6 * time.Hour, "6h"},
		{24 * time.Hour, "1d"},
		{4 * 24 * time.Hour, "4d"},
		{28 * 24 * time.Hour, "4w"},
	}

	for _, tc := range testcases {
		t.Run(tc.expected, func(t *testing.T) {
			result := formatWindowDuration(tc.duration)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestCalculateBurnrateWindows(t *testing.T) {
	// Test with standard 28-day window
	sloWindow := 28 * 24 * time.Hour

	windows := CalculateBurnrateWindows(sloWindow)
	require.Len(t, windows, 4)

	// Verify the first window pair (fastest burn rate)
	require.Equal(t, 5*time.Minute, windows[0].Short)
	require.Equal(t, time.Hour, windows[0].Long)
	require.Equal(t, float64(14), windows[0].Factor)

	// Verify the second window pair
	require.Equal(t, 30*time.Minute, windows[1].Short)
	require.Equal(t, 6*time.Hour, windows[1].Long)
	require.Equal(t, float64(7), windows[1].Factor)

	// Verify the third window pair
	require.Equal(t, 2*time.Hour, windows[2].Short)
	require.Equal(t, 24*time.Hour, windows[2].Long)
	require.Equal(t, float64(2), windows[2].Factor)

	// Verify the fourth window pair (slowest burn rate)
	require.Equal(t, 6*time.Hour, windows[3].Short)
	require.Equal(t, 4*24*time.Hour, windows[3].Long)
	require.Equal(t, float64(1), windows[3].Factor)
}

// TestCalculateBurnrateWindows_MatchesPrometheus verifies that CalculateBurnrateWindows
// produces the same Short/Long/Factor values as the canonical slo.Windows() function
// for all common SLO durations. This ensures the ClickHouse MV windows are consistent
// with the Prometheus recording rules.
func TestCalculateBurnrateWindows_MatchesPrometheus(t *testing.T) {
	commonDurations := []time.Duration{
		7 * 24 * time.Hour,    // 7 days
		14 * 24 * time.Hour,   // 14 days
		28 * 24 * time.Hour,   // 28 days (standard)
		30 * 24 * time.Hour,   // 30 days
		90 * 24 * time.Hour,   // 90 days
		365 * 24 * time.Hour,  // 365 days
	}

	for _, d := range commonDurations {
		t.Run(fmt.Sprintf("%dd", int(d.Hours()/24)), func(t *testing.T) {
			chWindows := CalculateBurnrateWindows(d)
			promWindows := slo.Windows(d)

			require.Equal(t, len(promWindows), len(chWindows),
				"number of windows should match for %v", d)

			for i := range promWindows {
				require.Equal(t, promWindows[i].Short, chWindows[i].Short,
					"Short window mismatch at index %d for SLO window %v: Prom=%v, CH=%v",
					i, d, promWindows[i].Short, chWindows[i].Short)
				require.Equal(t, promWindows[i].Long, chWindows[i].Long,
					"Long window mismatch at index %d for SLO window %v: Prom=%v, CH=%v",
					i, d, promWindows[i].Long, chWindows[i].Long)
				require.Equal(t, promWindows[i].Factor, chWindows[i].Factor,
					"Factor mismatch at index %d for SLO window %v: Prom=%v, CH=%v",
					i, d, promWindows[i].Factor, chWindows[i].Factor)
			}
		})
	}
}

func TestMatchersToSQLConditions(t *testing.T) {
	testcases := []struct {
		name     string
		matchers []*labels.Matcher
		expected string
	}{
		{
			name:     "empty",
			matchers: nil,
			expected: "1=1",
		},
		{
			name: "single equal",
			matchers: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "job", Value: "api"},
			},
			expected: "labels['job'] = 'api'",
		},
		{
			name: "multiple matchers",
			matchers: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "job", Value: "api"},
				{Type: labels.MatchRegexp, Name: "code", Value: "5.."},
			},
			expected: "labels['job'] = 'api' AND match(labels['code'], '5..')",
		},
		{
			name: "skip __name__",
			matchers: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: labels.MetricName, Value: "http_requests_total"},
				{Type: labels.MatchEqual, Name: "job", Value: "api"},
			},
			expected: "labels['job'] = 'api'",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			result := matchersToSQLConditions(tc.matchers)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestGroupingToSQLParts(t *testing.T) {
	testcases := []struct {
		name            string
		grouping        []string
		expectedExpr    string
		expectedGroupBy string
	}{
		{
			name:            "empty",
			grouping:        nil,
			expectedExpr:    "map()",
			expectedGroupBy: "",
		},
		{
			name:            "single",
			grouping:        []string{"handler"},
			expectedExpr:    "map('handler', labels['handler'])",
			expectedGroupBy: "GROUP BY labels['handler']",
		},
		{
			name:            "multiple",
			grouping:        []string{"handler", "method"},
			expectedExpr:    "map('handler', labels['handler'], 'method', labels['method'])",
			expectedGroupBy: "GROUP BY labels['handler'], labels['method']",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			expr, groupBy := groupingToSQLParts(tc.grouping)
			require.Equal(t, tc.expectedExpr, expr)
			require.Equal(t, tc.expectedGroupBy, groupBy)
		})
	}
}

func TestStaticLabelsFromMatchers(t *testing.T) {
	testcases := []struct {
		name     string
		matchers []*labels.Matcher
		grouping []string
		expected string
	}{
		{
			name:     "empty",
			matchers: nil,
			grouping: nil,
			expected: "",
		},
		{
			name: "single static label",
			matchers: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "job", Value: "api"},
			},
			grouping: nil,
			expected: ", map('job', 'api')",
		},
		{
			name: "exclude grouping label",
			matchers: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "job", Value: "api"},
				{Type: labels.MatchEqual, Name: "handler", Value: "/api/v1"},
			},
			grouping: []string{"handler"},
			expected: ", map('job', 'api')",
		},
		{
			name: "skip non-equal matchers",
			matchers: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "job", Value: "api"},
				{Type: labels.MatchRegexp, Name: "code", Value: "5.."},
			},
			grouping: nil,
			expected: ", map('job', 'api')",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			result := staticLabelsFromMatchers(tc.matchers, tc.grouping)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestMVGenerator_GenerateMVs_RatioIndicator(t *testing.T) {
	gen := NewMVGenerator(30 * time.Second)

	obj := slo.Objective{
		Labels: labels.FromStrings(labels.MetricName, "test-slo"),
		Window: model.Duration(28 * 24 * time.Hour),
		Indicator: slo.Indicator{
			Ratio: &slo.RatioIndicator{
				Errors: slo.Metric{
					Name: "http_requests_total",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "api"},
						{Type: labels.MatchRegexp, Name: "code", Value: "5.."},
					},
				},
				Total: slo.Metric{
					Name: "http_requests_total",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "api"},
					},
				},
				Grouping: []string{"handler"},
			},
		},
	}

	mvs, err := gen.GenerateMVs(obj)
	require.NoError(t, err)
	require.Len(t, mvs, 1, "should produce exactly 1 unified MV")

	mv := mvs[0]
	require.Equal(t, "slo_test_slo_mv", mv.Name)
	require.Contains(t, mv.SQL, "CREATE MATERIALIZED VIEW")
	require.Contains(t, mv.SQL, "REFRESH EVERY 30 SECOND")
	require.Contains(t, mv.SQL, "TO slo_recordings")
	require.Contains(t, mv.SQL, "UNION ALL")

	// Total increase query
	require.Contains(t, mv.SQL, "http_requests:increase4w")
	// Error increase query
	require.Contains(t, mv.SQL, "http_requests:error_increase4w")
	require.Contains(t, mv.SQL, "match(m.labels['code'], '5..')")
	// Burnrate queries
	require.Contains(t, mv.SQL, ":burnrate")
	require.Contains(t, mv.SQL, "map('slo', 'test-slo')")

	// Count UNION ALL branches: should be 2 increase + 7 burnrate = 9
	require.Equal(t, 8, strings.Count(mv.SQL, "UNION ALL"), "should have 9 sub-queries (8 UNION ALL separators)")
}

func TestMVGenerator_GenerateMVs_LatencyIndicator(t *testing.T) {
	gen := NewMVGenerator(30 * time.Second)

	obj := slo.Objective{
		Labels: labels.FromStrings(labels.MetricName, "latency-slo"),
		Window: model.Duration(28 * 24 * time.Hour),
		Indicator: slo.Indicator{
			Latency: &slo.LatencyIndicator{
				Total: slo.Metric{
					Name: "http_request_duration_count",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "api"},
					},
				},
				Success: slo.Metric{
					Name: "http_request_duration_bucket",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "api"},
						{Type: labels.MatchEqual, Name: "le", Value: "0.1"},
					},
				},
				Grouping: []string{"handler"},
			},
		},
	}

	mvs, err := gen.GenerateMVs(obj)
	require.NoError(t, err)
	require.Len(t, mvs, 1, "should produce exactly 1 unified MV")

	mv := mvs[0]
	// Total increase query
	require.Contains(t, mv.SQL, "http_request_duration:increase4w")
	// Success increase query (distinct name)
	require.Contains(t, mv.SQL, "http_request_duration:success_increase4w")
	// Burnrate queries
	require.Contains(t, mv.SQL, ":burnrate")
}

func TestMVGenerator_GenerateMVs_BoolGaugeIndicator(t *testing.T) {
	gen := NewMVGenerator(30 * time.Second)

	obj := slo.Objective{
		Labels: labels.FromStrings(labels.MetricName, "probe-slo"),
		Window: model.Duration(28 * 24 * time.Hour),
		Indicator: slo.Indicator{
			BoolGauge: &slo.BoolGaugeIndicator{
				Metric: slo.Metric{
					Name: "probe_success",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "blackbox"},
					},
				},
				Grouping: []string{"instance"},
			},
		},
	}

	mvs, err := gen.GenerateMVs(obj)
	require.NoError(t, err)
	require.Len(t, mvs, 1, "should produce exactly 1 unified MV")

	mv := mvs[0]
	// Count query
	require.Contains(t, mv.SQL, "count()")
	// Sum query
	require.Contains(t, mv.SQL, "sum(m.value)")
	// Burnrate queries
	require.Contains(t, mv.SQL, ":burnrate")
}

func TestMVGenerator_GeneratedSQL_Valid(t *testing.T) {
	gen := NewMVGenerator(30 * time.Second)

	obj := slo.Objective{
		Labels: labels.FromStrings(labels.MetricName, "test-slo"),
		Window: model.Duration(28 * 24 * time.Hour),
		Indicator: slo.Indicator{
			Ratio: &slo.RatioIndicator{
				Errors: slo.Metric{
					Name: "http_requests_total",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "api"},
						{Type: labels.MatchRegexp, Name: "code", Value: "5.."},
					},
				},
				Total: slo.Metric{
					Name: "http_requests_total",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "api"},
					},
				},
			},
		},
	}

	mvs, err := gen.GenerateMVs(obj)
	require.NoError(t, err)

	// Verify SQL structure for each MV
	for _, mv := range mvs {
		// Check for required SQL keywords
		require.Contains(t, mv.SQL, "CREATE MATERIALIZED VIEW", "MV %s missing CREATE", mv.Name)
		require.Contains(t, mv.SQL, "REFRESH EVERY", "MV %s missing REFRESH", mv.Name)
		require.Contains(t, mv.SQL, "TO slo_recordings", "MV %s missing TO", mv.Name)
		require.Contains(t, mv.SQL, "AS", "MV %s missing AS", mv.Name)
		require.Contains(t, mv.SQL, "SELECT", "MV %s missing SELECT", mv.Name)
		require.Contains(t, mv.SQL, "FROM metrics_raw", "MV %s missing FROM", mv.Name)
		require.Contains(t, mv.SQL, "WHERE", "MV %s missing WHERE", mv.Name)

		// Check for proper label handling
		require.Contains(t, mv.SQL, "map('slo', 'test-slo')", "MV %s missing slo label", mv.Name)

		// Verify no SQL injection issues (no unescaped quotes)
		sqlParts := strings.Split(mv.SQL, "'")
		for i, part := range sqlParts {
			if i%2 == 1 { // Inside single quotes
				require.NotContains(t, part, "\"", "MV %s has unescaped quotes", mv.Name)
			}
		}
	}
}

func TestMVGenerator_EscapesSQLInjection(t *testing.T) {
	gen := NewMVGenerator(30 * time.Second)

	// Test with malicious label value
	obj := slo.Objective{
		Labels: labels.FromStrings(labels.MetricName, "test'; DROP TABLE metrics_raw; --"),
		Window: model.Duration(28 * 24 * time.Hour),
		Indicator: slo.Indicator{
			Ratio: &slo.RatioIndicator{
				Errors: slo.Metric{
					Name: "http_requests_total",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "api'; DROP TABLE metrics_raw; --"},
					},
				},
				Total: slo.Metric{
					Name: "http_requests_total",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "api"},
					},
				},
			},
		},
	}

	mvs, err := gen.GenerateMVs(obj)
	require.NoError(t, err)

	for _, mv := range mvs {
		// Ensure the SQL properly escapes single quotes ('' instead of ')
		// The escaped version should be 'api''; DROP TABLE metrics_raw; --'
		// The key is that single quotes should be doubled (escaped)
		require.Contains(t, mv.SQL, "''", "should escape single quotes")

		// When properly escaped, the label value becomes:
		// 'test''; DROP TABLE metrics_raw; --' (note the doubled quote after 'test')
		// This is safe SQL because the quote is escaped, it doesn't end the string literal
		require.Contains(t, mv.SQL, "test''", "SLO name quotes should be escaped")
	}
}

