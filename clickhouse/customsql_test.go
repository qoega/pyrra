package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateCustomSQL(t *testing.T) {
	testcases := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "empty is valid",
			sql:     "",
			wantErr: false,
		},
		{
			name:    "valid select",
			sql:     "SELECT timestamp, labels, value FROM metrics_raw WHERE metric_name = 'test'",
			wantErr: false,
		},
		{
			name:    "drop forbidden",
			sql:     "DROP TABLE metrics_raw; SELECT 1",
			wantErr: true,
		},
		{
			name:    "truncate forbidden",
			sql:     "TRUNCATE TABLE metrics_raw",
			wantErr: true,
		},
		{
			name:    "delete forbidden",
			sql:     "DELETE FROM metrics_raw WHERE 1=1",
			wantErr: true,
		},
		{
			name:    "alter forbidden",
			sql:     "ALTER TABLE metrics_raw ADD COLUMN evil String",
			wantErr: true,
		},
		{
			name:    "insert forbidden",
			sql:     "INSERT INTO metrics_raw SELECT * FROM other",
			wantErr: true,
		},
		{
			name:    "update forbidden",
			sql:     "UPDATE metrics_raw SET value = 0",
			wantErr: true,
		},
		{
			name:    "grant forbidden",
			sql:     "GRANT ALL ON metrics_raw TO evil",
			wantErr: true,
		},
		{
			name:    "create forbidden",
			sql:     "CREATE TABLE evil (id Int32)",
			wantErr: true,
		},
		{
			name:    "no select forbidden",
			sql:     "SHOW TABLES",
			wantErr: true,
		},
		{
			name:    "case insensitive detection",
			sql:     "DROP table metrics_raw",
			wantErr: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCustomSQL(tc.sql)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSQLQueryExecutor_ExpandTemplate(t *testing.T) {
	executor := NewSQLQueryExecutor(nil, nil)

	params := SQLQueryParams{
		StartTime: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC),
		Step:      time.Minute,
		SLOName:   "test-slo",
		Namespace: "monitoring",
		Grouping:  []string{"handler", "method"},
	}

	template := `SELECT * FROM metrics_raw 
WHERE timestamp >= {{.StartTime}} 
  AND timestamp <= {{.EndTime}}
  AND slo = '{{.SLOName}}'
  AND namespace = '{{.Namespace}}'
  AND step = {{.Step}}
  AND grouping IN {{.Grouping}}`

	result := executor.expandTemplate(template, params)

	require.Contains(t, result, "toDateTime('2024-01-15 10:00:00')")
	require.Contains(t, result, "toDateTime('2024-01-15 11:00:00')")
	require.Contains(t, result, "slo = 'test-slo'")
	require.Contains(t, result, "namespace = 'monitoring'")
	require.Contains(t, result, "step = 60")
	require.Contains(t, result, "['handler', 'method']")
}

func TestSQLQueryExecutor_ExpandTemplate_EmptyGrouping(t *testing.T) {
	executor := NewSQLQueryExecutor(nil, nil)

	params := SQLQueryParams{
		StartTime: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC),
		Grouping:  nil,
	}

	template := `SELECT * FROM metrics_raw WHERE grouping IN {{.Grouping}}`

	result := executor.expandTemplate(template, params)
	require.Contains(t, result, "grouping IN []")
}

func TestSQLQueryExecutor_ExpandTemplate_SQLEscape(t *testing.T) {
	executor := NewSQLQueryExecutor(nil, nil)

	// Test SQL injection prevention
	params := SQLQueryParams{
		SLOName: "test'; DROP TABLE metrics_raw; --",
	}

	template := `SELECT * FROM metrics_raw WHERE slo = '{{.SLOName}}'`

	result := executor.expandTemplate(template, params)

	// Should have escaped quotes
	require.Contains(t, result, "''")
	require.Contains(t, result, "test''")
}

func TestGenerateDefaultTotalSQL(t *testing.T) {
	sql := GenerateDefaultTotalSQL(
		"http_requests_total",
		"labels['job'] = 'api'",
		[]string{"handler"},
	)

	require.Contains(t, sql, "SELECT")
	require.Contains(t, sql, "sum(value)")
	require.Contains(t, sql, "http_requests_total")
	require.Contains(t, sql, "labels['job'] = 'api'")
	require.Contains(t, sql, "GROUP BY")
	require.Contains(t, sql, "labels['handler']")
}

func TestGenerateDefaultErrorsSQL(t *testing.T) {
	sql := GenerateDefaultErrorsSQL(
		"http_requests_total",
		"labels['job'] = 'api'",
		"match(labels['code'], '^5')",
		[]string{"handler"},
	)

	require.Contains(t, sql, "SELECT")
	require.Contains(t, sql, "sumIf")
	require.Contains(t, sql, "match(labels['code'], '^5')")
	require.Contains(t, sql, "http_requests_total")
}

func TestGenerateDefaultRangeSQL(t *testing.T) {
	sql := GenerateDefaultRangeSQL(
		"http_requests_total",
		"labels['job'] = 'api'",
		[]string{"handler"},
	)

	require.Contains(t, sql, "SELECT")
	require.Contains(t, sql, "toStartOfMinute(timestamp)")
	require.Contains(t, sql, "{{.StartTime}}")
	require.Contains(t, sql, "{{.EndTime}}")
	require.Contains(t, sql, "ORDER BY timestamp")
}

func TestGenerateGroupingParts(t *testing.T) {
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
			expectedGroupBy: ", labels['handler']",
		},
		{
			name:            "multiple",
			grouping:        []string{"handler", "method"},
			expectedExpr:    "map('handler', labels['handler'], 'method', labels['method'])",
			expectedGroupBy: ", labels['handler'], labels['method']",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			expr, groupBy := generateGroupingParts(tc.grouping)
			require.Equal(t, tc.expectedExpr, expr)
			require.Equal(t, tc.expectedGroupBy, groupBy)
		})
	}
}

func TestExampleCustomSQLConfig(t *testing.T) {
	// Verify example configs are valid
	require.NoError(t, ValidateCustomSQL(ExampleCustomSQLConfig.RequestRangeSQL))
	require.NoError(t, ValidateCustomSQL(ExampleCustomSQLConfig.ErrorsRangeSQL))
}

// Integration test
func TestSQLQueryExecutor_Integration(t *testing.T) {
	client := setupClickHouseWithMigrations(t)

	ctx := context.Background()
	executor := NewSQLQueryExecutor(client, nil)

	// Insert test data
	err := client.Exec(ctx, `
		INSERT INTO metrics_raw (timestamp, metric_name, labels, value)
		VALUES 
			(now(), 'http_requests_total', {'job': 'api', 'handler': '/health'}, 100),
			(now(), 'http_requests_total', {'job': 'api', 'handler': '/api'}, 200)
	`)
	require.NoError(t, err)

	// Test instant query
	sql := `SELECT labels, value FROM metrics_raw WHERE metric_name = 'http_requests_total'`
	result, _, err := executor.ExecuteInstantQuery(ctx, sql, SQLQueryParams{}, time.Now())
	require.NoError(t, err)
	require.Len(t, result, 2)

	// Test range query
	rangeSql := `
		SELECT timestamp, labels, value 
		FROM metrics_raw 
		WHERE timestamp >= {{.StartTime}} AND timestamp <= {{.EndTime}}
		  AND metric_name = 'http_requests_total'
		ORDER BY timestamp
	`
	params := SQLQueryParams{
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now().Add(time.Hour),
	}
	matrix, _, err := executor.ExecuteRangeQuery(ctx, rangeSql, params)
	require.NoError(t, err)
	require.NotEmpty(t, matrix)
}



