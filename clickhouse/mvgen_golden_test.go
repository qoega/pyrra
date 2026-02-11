package clickhouse

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/pyrra-dev/pyrra/slo"
	"github.com/stretchr/testify/require"
)

func goldenDir() string {
	return filepath.Join("testdata", "golden")
}

func shouldUpdateGolden() bool {
	return os.Getenv("UPDATE_GOLDEN") == "1"
}

func assertOrUpdateGolden(t *testing.T, filename string, actual string) {
	t.Helper()

	path := filepath.Join(goldenDir(), filename)

	if shouldUpdateGolden() {
		err := os.MkdirAll(goldenDir(), 0o755)
		require.NoError(t, err)
		err = os.WriteFile(path, []byte(actual), 0o644)
		require.NoError(t, err)
		return
	}

	expected, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Fatalf("golden file %s does not exist; run with UPDATE_GOLDEN=1 to create it", path)
	}
	require.NoError(t, err)
	require.Equal(t, string(expected), actual, "golden file %s mismatch; run with UPDATE_GOLDEN=1 to update", filename)
}

// --- Test objectives for golden file tests ---

func goldenRatioObjective() slo.Objective {
	return slo.Objective{
		Labels: labels.FromStrings(labels.MetricName, "monitoring-http-errors"),
		Window: model.Duration(28 * 24 * time.Hour),
		Target: 0.99,
		Indicator: slo.Indicator{
			Ratio: &slo.RatioIndicator{
				Errors: slo.Metric{
					Name: "http_requests_total",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "thanos-receive-default"},
						{Type: labels.MatchRegexp, Name: "code", Value: "5.."},
					},
				},
				Total: slo.Metric{
					Name: "http_requests_total",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "thanos-receive-default"},
					},
				},
			},
		},
	}
}

func goldenRatioGroupingObjective() slo.Objective {
	return slo.Objective{
		Labels: labels.FromStrings(labels.MetricName, "monitoring-http-errors-by-handler"),
		Window: model.Duration(28 * 24 * time.Hour),
		Target: 0.999,
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
}

func goldenLatencyObjective() slo.Objective {
	return slo.Objective{
		Labels: labels.FromStrings(labels.MetricName, "monitoring-http-latency"),
		Window: model.Duration(28 * 24 * time.Hour),
		Target: 0.995,
		Indicator: slo.Indicator{
			Latency: &slo.LatencyIndicator{
				Success: slo.Metric{
					Name: "http_request_duration_seconds_bucket",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "api"},
						{Type: labels.MatchEqual, Name: "le", Value: "1"},
					},
				},
				Total: slo.Metric{
					Name: "http_request_duration_seconds_count",
					LabelMatchers: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "job", Value: "api"},
					},
				},
			},
		},
	}
}

func goldenBoolGaugeObjective() slo.Objective {
	return slo.Objective{
		Labels: labels.FromStrings(labels.MetricName, "monitoring-probe-success"),
		Window: model.Duration(28 * 24 * time.Hour),
		Target: 0.99,
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
}

// --- Golden file tests (one golden file per indicator type) ---

func TestGolden_RatioMV(t *testing.T) {
	gen := NewMVGenerator(30 * time.Second)
	mvs, err := gen.GenerateMVs(goldenRatioObjective())
	require.NoError(t, err)
	require.Len(t, mvs, 1)
	assertOrUpdateGolden(t, "ratio_unified.sql", mvs[0].SQL)
}

func TestGolden_RatioGroupingMV(t *testing.T) {
	gen := NewMVGenerator(30 * time.Second)
	mvs, err := gen.GenerateMVs(goldenRatioGroupingObjective())
	require.NoError(t, err)
	require.Len(t, mvs, 1)
	assertOrUpdateGolden(t, "ratio_grouping_unified.sql", mvs[0].SQL)
}

func TestGolden_LatencyMV(t *testing.T) {
	gen := NewMVGenerator(30 * time.Second)
	mvs, err := gen.GenerateMVs(goldenLatencyObjective())
	require.NoError(t, err)
	require.Len(t, mvs, 1)
	assertOrUpdateGolden(t, "latency_unified.sql", mvs[0].SQL)
}

func TestGolden_BoolGaugeMV(t *testing.T) {
	gen := NewMVGenerator(30 * time.Second)
	mvs, err := gen.GenerateMVs(goldenBoolGaugeObjective())
	require.NoError(t, err)
	require.Len(t, mvs, 1)
	assertOrUpdateGolden(t, "boolgauge_unified.sql", mvs[0].SQL)
}

// TestGolden_AllIndicatorTypes verifies every indicator type produces exactly 1 MV
// whose UNION ALL body covers both recording metrics and burnrate windows.
func TestGolden_AllIndicatorTypes(t *testing.T) {
	gen := NewMVGenerator(30 * time.Second)

	testcases := []struct {
		name      string
		objective slo.Objective
		// minUnions is the minimum number of UNION ALL separators expected.
		// For 28d window: 7 unique burnrate windows + N recording metrics.
		//   ratio:     2 increase + 7 burnrate => 9 branches => 8 UNION ALLs
		//   latency:   2 increase + 7 burnrate => 9 branches => 8 UNION ALLs
		//   boolgauge: 2 (count+sum) + 7 burnrate => 9 branches => 8 UNION ALLs
		minUnions int
	}{
		{
			name:      "ratio",
			objective: goldenRatioObjective(),
			minUnions: 8,
		},
		{
			name:      "ratio_grouping",
			objective: goldenRatioGroupingObjective(),
			minUnions: 8,
		},
		{
			name:      "latency",
			objective: goldenLatencyObjective(),
			minUnions: 8,
		},
		{
			name:      "boolgauge",
			objective: goldenBoolGaugeObjective(),
			minUnions: 8,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			mvs, err := gen.GenerateMVs(tc.objective)
			require.NoError(t, err)
			require.Len(t, mvs, 1, "should produce exactly 1 unified MV for %s", tc.name)

			mv := mvs[0]
			require.NotEmpty(t, mv.Name)
			require.Contains(t, mv.SQL, "CREATE MATERIALIZED VIEW")
			require.Contains(t, mv.SQL, "REFRESH EVERY")
			require.Contains(t, mv.SQL, "TO slo_recordings")
			require.Contains(t, mv.SQL, "UNION ALL")

			unionCount := strings.Count(mv.SQL, "UNION ALL")
			require.GreaterOrEqual(t, unionCount, tc.minUnions,
				"expected at least %d UNION ALL for %s, got %d", tc.minUnions, tc.name, unionCount)
		})
	}
}
