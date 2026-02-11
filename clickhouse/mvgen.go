package clickhouse

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/pyrra-dev/pyrra/slo"
)

// MVGenerator generates ClickHouse MV SQL from SLO definitions
type MVGenerator struct {
	refreshInterval time.Duration
}

// NewMVGenerator creates a new MV generator
func NewMVGenerator(refreshInterval time.Duration) *MVGenerator {
	return &MVGenerator{refreshInterval: refreshInterval}
}

// GeneratedMV represents a generated materialized view
type GeneratedMV struct {
	Name string
	SQL  string
}

// GenerateMVs generates a single unified MV for an SLO objective.
// All recording metrics (increase/error/success/count/sum + burnrates) are combined
// into one Refreshable Materialized View using UNION ALL.
func (g *MVGenerator) GenerateMVs(obj slo.Objective) ([]GeneratedMV, error) {
	safeName := sanitizeMVName(obj.Name())

	queries, err := g.collectQueries(obj)
	if err != nil {
		return nil, err
	}
	if len(queries) == 0 {
		return nil, fmt.Errorf("no queries generated for objective %s", obj.Name())
	}

	body := strings.Join(queries, "\n\nUNION ALL\n\n")

	sql := fmt.Sprintf("CREATE MATERIALIZED VIEW slo_%s_mv\nREFRESH EVERY %d SECOND APPEND\nTO slo_recordings\nAS\n%s",
		safeName,
		int(g.refreshInterval.Seconds()),
		body,
	)

	return []GeneratedMV{{
		Name: fmt.Sprintf("slo_%s_mv", safeName),
		SQL:  sql,
	}}, nil
}

// collectQueries returns all SELECT statements for the objective, one per metric.
func (g *MVGenerator) collectQueries(obj slo.Objective) ([]string, error) {
	var queries []string

	increaseQueries, err := g.increaseQueries(obj)
	if err != nil {
		return nil, fmt.Errorf("increase queries: %w", err)
	}
	queries = append(queries, increaseQueries...)

	burnrateQueries, err := g.burnrateQueries(obj)
	if err != nil {
		return nil, fmt.Errorf("burnrate queries: %w", err)
	}
	queries = append(queries, burnrateQueries...)

	return queries, nil
}

// ---------------------------------------------------------------------------
// Increase / count / sum SELECT builders
// ---------------------------------------------------------------------------

func (g *MVGenerator) increaseQueries(obj slo.Objective) ([]string, error) {
	sloName := obj.Name()
	window := time.Duration(obj.Window)
	windowStr := formatWindowDuration(window)

	switch obj.IndicatorType() {
	case slo.Ratio:
		totalMetric := obj.Indicator.Ratio.Total.Name
		baseMetric := extractBaseMetric(totalMetric)
		grouping := obj.Indicator.Ratio.Grouping

		totalQ := increaseSelectQuery(
			fmt.Sprintf("%s:increase%s", baseMetric, windowStr),
			sloName, totalMetric,
			obj.Indicator.Ratio.Total.LabelMatchers,
			grouping, window,
		)

		errorMetric := obj.Indicator.Ratio.Errors.Name
		errorBase := extractBaseMetric(errorMetric)
		errorQ := increaseSelectQuery(
			fmt.Sprintf("%s:error_increase%s", errorBase, windowStr),
			sloName, errorMetric,
			obj.Indicator.Ratio.Errors.LabelMatchers,
			grouping, window,
		)

		return []string{totalQ, errorQ}, nil

	case slo.Latency:
		totalMetric := obj.Indicator.Latency.Total.Name
		baseMetric := extractBaseMetric(totalMetric)
		grouping := obj.Indicator.Latency.Grouping

		totalQ := increaseSelectQuery(
			fmt.Sprintf("%s:increase%s", baseMetric, windowStr),
			sloName, totalMetric,
			obj.Indicator.Latency.Total.LabelMatchers,
			grouping, window,
		)

		successMetric := obj.Indicator.Latency.Success.Name
		successBase := extractBaseMetric(successMetric)
		successQ := increaseSelectQuery(
			fmt.Sprintf("%s:success_increase%s", successBase, windowStr),
			sloName, successMetric,
			obj.Indicator.Latency.Success.LabelMatchers,
			grouping, window,
		)

		return []string{totalQ, successQ}, nil

	case slo.LatencyNative:
		totalMetric := obj.Indicator.LatencyNative.Total.Name
		baseMetric := extractBaseMetric(totalMetric)
		grouping := obj.Indicator.LatencyNative.Grouping

		totalQ := increaseSelectQuery(
			fmt.Sprintf("%s:increase%s", baseMetric, windowStr),
			sloName, totalMetric,
			obj.Indicator.LatencyNative.Total.LabelMatchers,
			grouping, window,
		)
		return []string{totalQ}, nil

	case slo.BoolGauge:
		metricName := obj.Indicator.BoolGauge.Metric.Name
		matchers := obj.Indicator.BoolGauge.Metric.LabelMatchers
		grouping := obj.Indicator.BoolGauge.Grouping

		countQ := countSelectQuery(
			fmt.Sprintf("%s:count%s", metricName, windowStr),
			sloName, metricName, matchers, grouping, window,
		)
		sumQ := sumSelectQuery(
			fmt.Sprintf("%s:sum%s", metricName, windowStr),
			sloName, metricName, matchers, grouping, window,
		)
		return []string{countQ, sumQ}, nil

	default:
		return nil, fmt.Errorf("unsupported indicator type: %v", obj.IndicatorType())
	}
}

// ---------------------------------------------------------------------------
// Burnrate SELECT builders
// ---------------------------------------------------------------------------

func (g *MVGenerator) burnrateQueries(obj slo.Objective) ([]string, error) {
	// Deduplicate and sort windows for deterministic output
	windows := CalculateBurnrateWindows(time.Duration(obj.Window))
	windowSet := make(map[time.Duration]bool)
	for _, w := range windows {
		windowSet[w.Short] = true
		windowSet[w.Long] = true
	}

	sorted := make([]time.Duration, 0, len(windowSet))
	for w := range windowSet {
		sorted = append(sorted, w)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var queries []string
	for _, window := range sorted {
		q, err := g.singleBurnrateQuery(obj, window)
		if err != nil {
			return nil, err
		}
		queries = append(queries, q)
	}
	return queries, nil
}

func (g *MVGenerator) singleBurnrateQuery(obj slo.Objective, window time.Duration) (string, error) {
	sloName := obj.Name()
	windowStr := formatWindowDuration(window)

	switch obj.IndicatorType() {
	case slo.Ratio:
		totalMetric := obj.Indicator.Ratio.Total.Name
		baseMetric := extractBaseMetric(totalMetric)
		return ratioBurnrateSelectQuery(
			fmt.Sprintf("%s:burnrate%s", baseMetric, windowStr),
			sloName, totalMetric,
			obj.Indicator.Ratio.Total.LabelMatchers,
			obj.Indicator.Ratio.Errors.LabelMatchers,
			obj.Indicator.Ratio.Grouping,
			window,
		), nil

	case slo.Latency, slo.LatencyNative:
		var totalMetric, successMetric string
		var totalMatchers, successMatchers []*labels.Matcher
		var grouping []string

		if obj.IndicatorType() == slo.Latency {
			totalMetric = obj.Indicator.Latency.Total.Name
			successMetric = obj.Indicator.Latency.Success.Name
			totalMatchers = obj.Indicator.Latency.Total.LabelMatchers
			successMatchers = obj.Indicator.Latency.Success.LabelMatchers
			grouping = obj.Indicator.Latency.Grouping
		} else {
			totalMetric = obj.Indicator.LatencyNative.Total.Name
			successMetric = totalMetric
			totalMatchers = obj.Indicator.LatencyNative.Total.LabelMatchers
			successMatchers = totalMatchers
			grouping = obj.Indicator.LatencyNative.Grouping
		}

		baseMetric := extractBaseMetric(totalMetric)
		return latencyBurnrateSelectQuery(
			fmt.Sprintf("%s:burnrate%s", baseMetric, windowStr),
			sloName, totalMetric, successMetric,
			totalMatchers, successMatchers,
			grouping, window,
		), nil

	case slo.BoolGauge:
		metricName := obj.Indicator.BoolGauge.Metric.Name
		return boolGaugeBurnrateSelectQuery(
			fmt.Sprintf("%s:burnrate%s", metricName, windowStr),
			sloName, metricName,
			obj.Indicator.BoolGauge.Metric.LabelMatchers,
			obj.Indicator.BoolGauge.Grouping,
			window,
		), nil

	default:
		return "", fmt.Errorf("unsupported indicator type: %v", obj.IndicatorType())
	}
}

// ---------------------------------------------------------------------------
// Pure SELECT query builders (no CREATE MATERIALIZED VIEW wrapper)
// ---------------------------------------------------------------------------

// increaseSelectQuery returns a SELECT that sums raw metric values over a window.
func increaseSelectQuery(
	recordingName, sloName, metricName string,
	matchers []*labels.Matcher,
	grouping []string,
	window time.Duration,
) string {
	matchersSQL := matchersToSQLConditionsWithAlias(matchers, "m")
	groupingExpr, groupByClause := groupingToSQLPartsWithAlias(grouping, "m")
	staticLabels := staticLabelsFromMatchers(matchers, grouping)

	return fmt.Sprintf(`SELECT
    toStartOfMinute(now()) AS timestamp,
    '%s' AS metric_name,
    mapConcat(
        %s,
        map('slo', '%s')%s
    ) AS labels,
    sum(m.value) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(%d)
  AND m.metric_name = '%s'
  AND %s
%s`,
		escapeSQLString(recordingName),
		groupingExpr,
		escapeSQLString(sloName),
		staticLabels,
		int(window.Seconds()),
		escapeSQLString(metricName),
		matchersSQL,
		groupByClause,
	)
}

// countSelectQuery returns a SELECT that counts raw metric rows over a window.
func countSelectQuery(
	recordingName, sloName, metricName string,
	matchers []*labels.Matcher,
	grouping []string,
	window time.Duration,
) string {
	matchersSQL := matchersToSQLConditionsWithAlias(matchers, "m")
	groupingExpr, groupByClause := groupingToSQLPartsWithAlias(grouping, "m")
	staticLabels := staticLabelsFromMatchers(matchers, grouping)

	return fmt.Sprintf(`SELECT
    toStartOfMinute(now()) AS timestamp,
    '%s' AS metric_name,
    mapConcat(
        %s,
        map('slo', '%s')%s
    ) AS labels,
    toFloat64(count()) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(%d)
  AND m.metric_name = '%s'
  AND %s
%s`,
		escapeSQLString(recordingName),
		groupingExpr,
		escapeSQLString(sloName),
		staticLabels,
		int(window.Seconds()),
		escapeSQLString(metricName),
		matchersSQL,
		groupByClause,
	)
}

// sumSelectQuery returns a SELECT that sums raw metric values over a window
// (same as increaseSelectQuery — kept as separate function for clarity with BoolGauge).
func sumSelectQuery(
	recordingName, sloName, metricName string,
	matchers []*labels.Matcher,
	grouping []string,
	window time.Duration,
) string {
	// Identical to increaseSelectQuery; separate for readability.
	return increaseSelectQuery(recordingName, sloName, metricName, matchers, grouping, window)
}

// ratioBurnrateSelectQuery returns a SELECT that computes error/total ratio.
func ratioBurnrateSelectQuery(
	recordingName, sloName, metricName string,
	totalMatchers, errorMatchers []*labels.Matcher,
	grouping []string,
	window time.Duration,
) string {
	totalMatchersSQL := matchersToSQLConditionsWithAlias(totalMatchers, "m")
	errorMatchersSQL := matchersToSQLConditionsWithAlias(errorMatchers, "m")
	groupingExpr, groupByClause := groupingToSQLPartsWithAlias(grouping, "m")
	staticLabels := staticLabelsFromMatchers(totalMatchers, grouping)

	return fmt.Sprintf(`SELECT
    toStartOfMinute(now()) AS timestamp,
    '%s' AS metric_name,
    mapConcat(
        %s,
        map('slo', '%s')%s
    ) AS labels,
    sumIf(m.value, %s) / nullIf(sumIf(m.value, %s), 0) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(%d)
  AND m.metric_name = '%s'
%s`,
		escapeSQLString(recordingName),
		groupingExpr,
		escapeSQLString(sloName),
		staticLabels,
		errorMatchersSQL,
		totalMatchersSQL,
		int(window.Seconds()),
		escapeSQLString(metricName),
		groupByClause,
	)
}

// latencyBurnrateSelectQuery returns a SELECT that computes 1 - success/total.
func latencyBurnrateSelectQuery(
	recordingName, sloName string,
	totalMetric, successMetric string,
	totalMatchers, successMatchers []*labels.Matcher,
	grouping []string,
	window time.Duration,
) string {
	totalMatchersSQL := matchersToSQLConditionsWithAlias(totalMatchers, "m")
	successMatchersSQL := matchersToSQLConditionsWithAlias(successMatchers, "m")
	groupingExpr, groupByClause := groupingToSQLPartsWithAlias(grouping, "m")
	staticLabels := staticLabelsFromMatchers(totalMatchers, grouping)

	return fmt.Sprintf(`SELECT
    toStartOfMinute(now()) AS timestamp,
    '%s' AS metric_name,
    mapConcat(
        %s,
        map('slo', '%s')%s
    ) AS labels,
    1 - (sumIf(m.value, m.metric_name = '%s' AND %s) / nullIf(sumIf(m.value, m.metric_name = '%s' AND %s), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(%d)
  AND m.metric_name IN ('%s', '%s')
%s`,
		escapeSQLString(recordingName),
		groupingExpr,
		escapeSQLString(sloName),
		staticLabels,
		escapeSQLString(successMetric),
		successMatchersSQL,
		escapeSQLString(totalMetric),
		totalMatchersSQL,
		int(window.Seconds()),
		escapeSQLString(successMetric),
		escapeSQLString(totalMetric),
		groupByClause,
	)
}

// boolGaugeBurnrateSelectQuery returns a SELECT that computes 1 - sum/count.
func boolGaugeBurnrateSelectQuery(
	recordingName, sloName, metricName string,
	matchers []*labels.Matcher,
	grouping []string,
	window time.Duration,
) string {
	matchersSQL := matchersToSQLConditionsWithAlias(matchers, "m")
	groupingExpr, groupByClause := groupingToSQLPartsWithAlias(grouping, "m")
	staticLabels := staticLabelsFromMatchers(matchers, grouping)

	return fmt.Sprintf(`SELECT
    toStartOfMinute(now()) AS timestamp,
    '%s' AS metric_name,
    mapConcat(
        %s,
        map('slo', '%s')%s
    ) AS labels,
    1 - (sum(m.value) / nullIf(count(), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(%d)
  AND m.metric_name = '%s'
  AND %s
%s`,
		escapeSQLString(recordingName),
		groupingExpr,
		escapeSQLString(sloName),
		staticLabels,
		int(window.Seconds()),
		escapeSQLString(metricName),
		matchersSQL,
		groupByClause,
	)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Window represents a burn rate window configuration
type Window struct {
	Short  time.Duration
	Long   time.Duration
	Factor float64
}

// CalculateBurnrateWindows calculates windows based on SLO window
// This matches the logic in slo/rules.go Windows()
func CalculateBurnrateWindows(sloWindow time.Duration) []Window {
	round := time.Minute

	return []Window{
		{
			Short:  (sloWindow / (28 * 24 * 12)).Round(round), // 5m for 28d
			Long:   (sloWindow / (28 * 24)).Round(round),      // 1h for 28d
			Factor: 14,
		},
		{
			Short:  (sloWindow / (28 * 24 * 2)).Round(round), // 30m for 28d
			Long:   (sloWindow / (28 * 4)).Round(round),      // 6h for 28d
			Factor: 7,
		},
		{
			Short:  (sloWindow / (28 * 12)).Round(round), // 2h for 28d
			Long:   (sloWindow / 28).Round(round),        // 1d for 28d
			Factor: 2,
		},
		{
			Short:  (sloWindow / (28 * 4)).Round(round), // 6h for 28d
			Long:   (sloWindow / 7).Round(round),        // 4d for 28d
			Factor: 1,
		},
	}
}

// matchersToSQLConditions converts label matchers to SQL WHERE conditions
func matchersToSQLConditions(matchers []*labels.Matcher) string {
	if len(matchers) == 0 {
		return "1=1"
	}

	var conditions []string
	for _, m := range matchers {
		if m.Name == labels.MetricName {
			continue // Handled separately
		}
		conditions = append(conditions, matcherToSQLCond(m))
	}

	if len(conditions) == 0 {
		return "1=1"
	}

	return strings.Join(conditions, " AND ")
}

// matcherToSQLCond converts a single label matcher to SQL condition (with full escaping)
func matcherToSQLCond(m *labels.Matcher) string {
	switch m.Type {
	case labels.MatchEqual:
		return fmt.Sprintf("labels['%s'] = '%s'", escapeSQLString(m.Name), escapeSQLString(m.Value))
	case labels.MatchNotEqual:
		return fmt.Sprintf("labels['%s'] != '%s'", escapeSQLString(m.Name), escapeSQLString(m.Value))
	case labels.MatchRegexp:
		return fmt.Sprintf("match(labels['%s'], '%s')", escapeSQLString(m.Name), escapeSQLString(m.Value))
	case labels.MatchNotRegexp:
		return fmt.Sprintf("NOT match(labels['%s'], '%s')", escapeSQLString(m.Name), escapeSQLString(m.Value))
	default:
		return "1=1"
	}
}

// groupingToSQLParts converts grouping labels to map expression and GROUP BY clause
func groupingToSQLParts(grouping []string) (string, string) {
	if len(grouping) == 0 {
		return "map()", ""
	}

	var pairs []string
	var groupCols []string
	for _, g := range grouping {
		pairs = append(pairs, fmt.Sprintf("'%s', labels['%s']", g, g))
		groupCols = append(groupCols, fmt.Sprintf("labels['%s']", g))
	}

	return fmt.Sprintf("map(%s)", strings.Join(pairs, ", ")),
		fmt.Sprintf("GROUP BY %s", strings.Join(groupCols, ", "))
}

// matchersToSQLConditionsWithAlias converts matchers to SQL conditions with table alias
func matchersToSQLConditionsWithAlias(matchers []*labels.Matcher, alias string) string {
	if len(matchers) == 0 {
		return "1=1"
	}

	var conditions []string
	for _, m := range matchers {
		if m.Name == labels.MetricName {
			continue // Handled separately
		}
		conditions = append(conditions, matcherToSQLCondWithAlias(m, alias))
	}

	if len(conditions) == 0 {
		return "1=1"
	}

	return strings.Join(conditions, " AND ")
}

// matcherToSQLCondWithAlias converts a single label matcher to SQL condition with table alias
func matcherToSQLCondWithAlias(m *labels.Matcher, alias string) string {
	col := fmt.Sprintf("%s.labels['%s']", alias, escapeSQLString(m.Name))
	switch m.Type {
	case labels.MatchEqual:
		return fmt.Sprintf("%s = '%s'", col, escapeSQLString(m.Value))
	case labels.MatchNotEqual:
		return fmt.Sprintf("%s != '%s'", col, escapeSQLString(m.Value))
	case labels.MatchRegexp:
		return fmt.Sprintf("match(%s, '%s')", col, escapeSQLString(m.Value))
	case labels.MatchNotRegexp:
		return fmt.Sprintf("NOT match(%s, '%s')", col, escapeSQLString(m.Value))
	default:
		return "1=1"
	}
}

// groupingToSQLPartsWithAlias converts grouping labels to map expression and GROUP BY clause with table alias
func groupingToSQLPartsWithAlias(grouping []string, alias string) (string, string) {
	if len(grouping) == 0 {
		return "map()", ""
	}

	var pairs []string
	var groupCols []string
	for _, g := range grouping {
		pairs = append(pairs, fmt.Sprintf("'%s', %s.labels['%s']", g, alias, g))
		groupCols = append(groupCols, fmt.Sprintf("%s.labels['%s']", alias, g))
	}

	return fmt.Sprintf("map(%s)", strings.Join(pairs, ", ")),
		fmt.Sprintf("GROUP BY %s", strings.Join(groupCols, ", "))
}

// staticLabelsFromMatchers extracts static labels (MatchEqual) that aren't in grouping
func staticLabelsFromMatchers(matchers []*labels.Matcher, grouping []string) string {
	groupingSet := make(map[string]bool)
	for _, g := range grouping {
		groupingSet[g] = true
	}

	var pairs []string
	for _, m := range matchers {
		if m.Type == labels.MatchEqual && m.Name != labels.MetricName && !groupingSet[m.Name] {
			pairs = append(pairs, fmt.Sprintf("'%s', '%s'", m.Name, escapeSQLString(m.Value)))
		}
	}

	if len(pairs) == 0 {
		return ""
	}

	return fmt.Sprintf(", map(%s)", strings.Join(pairs, ", "))
}

// sanitizeMVName converts SLO name to valid ClickHouse identifier
func sanitizeMVName(name string) string {
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, " ", "_")
	return name
}

// extractBaseMetric extracts the base metric name without _total, _count, _bucket suffixes
func extractBaseMetric(metricName string) string {
	for _, suffix := range []string{"_total", "_count", "_bucket", "_sum"} {
		if strings.HasSuffix(metricName, suffix) {
			return strings.TrimSuffix(metricName, suffix)
		}
	}
	return metricName
}

// formatWindowDuration formats a duration for use in metric names (e.g., "5m", "1h", "4d", "4w")
func formatWindowDuration(d time.Duration) string {
	// Use Prometheus model.Duration for consistent formatting
	return model.Duration(d).String()
}

// formatWindowForName formats a duration for use in MV names (safe for identifiers)
func formatWindowForName(d time.Duration) string {
	s := formatWindowDuration(d)
	return strings.ReplaceAll(s, ".", "_")
}
