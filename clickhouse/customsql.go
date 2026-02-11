package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	prometheusapiv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// CustomSQLConfig holds custom SQL queries for an SLO
type CustomSQLConfig struct {
	// RequestRangeSQL is a custom SQL query for fetching request rates over time
	// The query should return columns: timestamp DateTime, labels Map(String, String), value Float64
	// Available placeholders: {{.StartTime}}, {{.EndTime}}, {{.Step}}, {{.SLOName}}, {{.Namespace}}
	RequestRangeSQL string `yaml:"requestRangeSQL,omitempty"`

	// ErrorsRangeSQL is a custom SQL query for fetching error rates over time
	// Same column requirements as RequestRangeSQL
	ErrorsRangeSQL string `yaml:"errorsRangeSQL,omitempty"`

	// DurationRangeSQL is a custom SQL query for fetching duration/latency data
	// Same column requirements as RequestRangeSQL
	DurationRangeSQL string `yaml:"durationRangeSQL,omitempty"`

	// TotalSQL is a custom SQL query for total request count (instant)
	// Should return columns: labels Map(String, String), value Float64
	TotalSQL string `yaml:"totalSQL,omitempty"`

	// ErrorsSQL is a custom SQL query for error count (instant)
	// Same column requirements as TotalSQL
	ErrorsSQL string `yaml:"errorsSQL,omitempty"`
}

// SQLQueryExecutor executes custom SQL queries for SLO metrics
type SQLQueryExecutor struct {
	client *Client
	logger log.Logger
}

// NewSQLQueryExecutor creates a new custom SQL query executor
func NewSQLQueryExecutor(client *Client, logger log.Logger) *SQLQueryExecutor {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &SQLQueryExecutor{
		client: client,
		logger: logger,
	}
}

// SQLQueryParams holds parameters for SQL template substitution
type SQLQueryParams struct {
	StartTime time.Time
	EndTime   time.Time
	Step      time.Duration
	SLOName   string
	Namespace string
	Grouping  []string
}

// ExecuteInstantQuery executes a custom SQL query and returns instant results
func (e *SQLQueryExecutor) ExecuteInstantQuery(
	ctx context.Context,
	sqlTemplate string,
	params SQLQueryParams,
	ts time.Time,
) (model.Vector, prometheusapiv1.Warnings, error) {
	sql := e.expandTemplate(sqlTemplate, params)
	level.Debug(e.logger).Log("msg", "executing custom instant SQL", "sql", sql)

	rows, err := e.client.Query(ctx, sql)
	if err != nil {
		return nil, nil, fmt.Errorf("execute custom SQL: %w", err)
	}
	defer rows.Close()

	return e.rowsToVector(rows, ts)
}

// ExecuteRangeQuery executes a custom SQL query and returns range results
func (e *SQLQueryExecutor) ExecuteRangeQuery(
	ctx context.Context,
	sqlTemplate string,
	params SQLQueryParams,
) (model.Matrix, prometheusapiv1.Warnings, error) {
	sql := e.expandTemplate(sqlTemplate, params)
	level.Debug(e.logger).Log("msg", "executing custom range SQL", "sql", sql)

	rows, err := e.client.Query(ctx, sql)
	if err != nil {
		return nil, nil, fmt.Errorf("execute custom SQL: %w", err)
	}
	defer rows.Close()

	return e.rowsToMatrix(rows)
}

// expandTemplate replaces template placeholders with actual values
func (e *SQLQueryExecutor) expandTemplate(template string, params SQLQueryParams) string {
	result := template

	// Replace time placeholders (use UTC to match ClickHouse server timezone)
	result = strings.ReplaceAll(result, "{{.StartTime}}", 
		fmt.Sprintf("toDateTime('%s')", params.StartTime.UTC().Format("2006-01-02 15:04:05")))
	result = strings.ReplaceAll(result, "{{.EndTime}}", 
		fmt.Sprintf("toDateTime('%s')", params.EndTime.UTC().Format("2006-01-02 15:04:05")))
	result = strings.ReplaceAll(result, "{{.Step}}", 
		fmt.Sprintf("%d", int(params.Step.Seconds())))

	// Replace SLO placeholders
	result = strings.ReplaceAll(result, "{{.SLOName}}", 
		escapeSQLString(params.SLOName))
	result = strings.ReplaceAll(result, "{{.Namespace}}", 
		escapeSQLString(params.Namespace))

	// Replace grouping placeholder
	if len(params.Grouping) > 0 {
		groupingList := strings.Join(params.Grouping, "', '")
		result = strings.ReplaceAll(result, "{{.Grouping}}", 
			fmt.Sprintf("['%s']", groupingList))
	} else {
		result = strings.ReplaceAll(result, "{{.Grouping}}", "[]")
	}

	return result
}

// rowsToVector converts ClickHouse rows to Prometheus Vector
func (e *SQLQueryExecutor) rowsToVector(rows driver.Rows, ts time.Time) (model.Vector, prometheusapiv1.Warnings, error) {
	var vector model.Vector

	for rows.Next() {
		var (
			labelsMap map[string]string
			value     float64
		)

		if err := rows.Scan(&labelsMap, &value); err != nil {
			return nil, nil, fmt.Errorf("scan row: %w", err)
		}

		metric := make(model.Metric, len(labelsMap))
		for k, v := range labelsMap {
			metric[model.LabelName(k)] = model.LabelValue(v)
		}

		vector = append(vector, &model.Sample{
			Metric:    metric,
			Value:     model.SampleValue(value),
			Timestamp: model.TimeFromUnixNano(ts.UnixNano()),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate rows: %w", err)
	}

	return vector, nil, nil
}

// rowsToMatrix converts ClickHouse rows to Prometheus Matrix
func (e *SQLQueryExecutor) rowsToMatrix(rows driver.Rows) (model.Matrix, prometheusapiv1.Warnings, error) {
	streams := make(map[uint64]*model.SampleStream)

	for rows.Next() {
		var (
			timestamp time.Time
			labelsMap map[string]string
			value     float64
		)

		if err := rows.Scan(&timestamp, &labelsMap, &value); err != nil {
			return nil, nil, fmt.Errorf("scan row: %w", err)
		}

		metric := make(model.Metric, len(labelsMap))
		for k, v := range labelsMap {
			metric[model.LabelName(k)] = model.LabelValue(v)
		}

		fp := metric.Fingerprint()
		stream, ok := streams[uint64(fp)]
		if !ok {
			stream = &model.SampleStream{
				Metric: metric,
				Values: make([]model.SamplePair, 0),
			}
			streams[uint64(fp)] = stream
		}

		stream.Values = append(stream.Values, model.SamplePair{
			Timestamp: model.TimeFromUnixNano(timestamp.UnixNano()),
			Value:     model.SampleValue(value),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate rows: %w", err)
	}

	matrix := make(model.Matrix, 0, len(streams))
	for _, stream := range streams {
		matrix = append(matrix, stream)
	}

	return matrix, nil, nil
}

// ValidateCustomSQL validates a custom SQL template
func ValidateCustomSQL(sqlTemplate string) error {
	if sqlTemplate == "" {
		return nil // Empty is valid (will use default)
	}

	// Check for dangerous SQL patterns
	lowerSQL := strings.ToLower(sqlTemplate)
	dangerousPatterns := []string{
		"drop ",
		"truncate ",
		"delete ",
		"alter ",
		"create ",
		"insert ",
		"update ",
		"grant ",
		"revoke ",
	}

	for _, pattern := range dangerousPatterns {
		if strings.Contains(lowerSQL, pattern) {
			return fmt.Errorf("custom SQL contains forbidden operation: %s", pattern)
		}
	}

	// Check for required SELECT
	if !strings.Contains(lowerSQL, "select") {
		return fmt.Errorf("custom SQL must be a SELECT query")
	}

	return nil
}

// GenerateDefaultTotalSQL generates a default SQL query for total count
func GenerateDefaultTotalSQL(metricName string, matchers string, grouping []string) string {
	groupingExpr, groupByClause := generateGroupingParts(grouping)
	
	groupBySQL := ""
	if groupByClause != "" {
		groupBySQL = fmt.Sprintf("GROUP BY %s", groupByClause[2:]) // Remove leading ", "
	}

	return fmt.Sprintf(`
SELECT 
    %s AS labels,
    sum(value) AS value
FROM metrics_raw
WHERE timestamp >= now() - INTERVAL 5 MINUTE
  AND metric_name = '%s'
  AND %s
%s`,
		groupingExpr,
		escapeSQLString(metricName),
		matchers,
		groupBySQL,
	)
}

// GenerateDefaultErrorsSQL generates a default SQL query for error count
func GenerateDefaultErrorsSQL(metricName string, totalMatchers string, errorMatchers string, grouping []string) string {
	groupingExpr, groupByClause := generateGroupingParts(grouping)
	
	groupBySQL := ""
	if groupByClause != "" {
		groupBySQL = fmt.Sprintf("GROUP BY %s", groupByClause[2:]) // Remove leading ", "
	}

	return fmt.Sprintf(`
SELECT 
    %s AS labels,
    sumIf(value, %s) AS value
FROM metrics_raw
WHERE timestamp >= now() - INTERVAL 5 MINUTE
  AND metric_name = '%s'
  AND %s
%s`,
		groupingExpr,
		errorMatchers,
		escapeSQLString(metricName),
		totalMatchers,
		groupBySQL,
	)
}

// GenerateDefaultRangeSQL generates a default SQL query for range data
func GenerateDefaultRangeSQL(metricName string, matchers string, grouping []string) string {
	groupingExpr, groupByClause := generateGroupingParts(grouping)

	return fmt.Sprintf(`
SELECT 
    toStartOfMinute(timestamp) AS timestamp,
    %s AS labels,
    sum(value) AS value
FROM metrics_raw
WHERE timestamp >= {{.StartTime}}
  AND timestamp <= {{.EndTime}}
  AND metric_name = '%s'
  AND %s
GROUP BY timestamp%s
ORDER BY timestamp`,
		groupingExpr,
		escapeSQLString(metricName),
		matchers,
		groupByClause,
	)
}

// generateGroupingParts generates SQL parts for grouping
func generateGroupingParts(grouping []string) (string, string) {
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
		fmt.Sprintf(", %s", strings.Join(groupCols, ", "))
}

// ExampleCustomSQLConfig provides example configurations for documentation
var ExampleCustomSQLConfig = CustomSQLConfig{
	RequestRangeSQL: `
SELECT 
    toStartOfMinute(timestamp) AS timestamp,
    map('slo', '{{.SLOName}}', 'handler', labels['handler']) AS labels,
    sum(value) AS value
FROM metrics_raw
WHERE timestamp >= {{.StartTime}}
  AND timestamp <= {{.EndTime}}
  AND metric_name = 'http_requests_total'
  AND labels['job'] = 'api-server'
GROUP BY timestamp, labels['handler']
ORDER BY timestamp`,

	ErrorsRangeSQL: `
SELECT 
    toStartOfMinute(timestamp) AS timestamp,
    map('slo', '{{.SLOName}}', 'handler', labels['handler']) AS labels,
    sumIf(value, match(labels['code'], '^5')) AS value
FROM metrics_raw
WHERE timestamp >= {{.StartTime}}
  AND timestamp <= {{.EndTime}}
  AND metric_name = 'http_requests_total'
  AND labels['job'] = 'api-server'
GROUP BY timestamp, labels['handler']
ORDER BY timestamp`,
}

