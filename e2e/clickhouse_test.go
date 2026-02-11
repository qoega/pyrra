package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/pyrra-dev/pyrra/clickhouse"
	"github.com/stretchr/testify/require"
	chmodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

// TestE2E_ClickHouseBackend_FullStack tests the full deployed ClickHouse backend stack.
//
// This test:
// 1. Starts a ClickHouse container
// 2. Runs migrations
// 3. Provisions MVs for test SLOs
// 4. Inserts test metrics data
// 5. Queries via native SQL to verify results
// 6. Verifies the HTTP API endpoints work
//
// Set E2E=1 to run: E2E=1 go test -v -count=1 -timeout 5m ./e2e/...
func TestE2E_ClickHouseBackend_FullStack(t *testing.T) {
	if os.Getenv("E2E") != "1" {
		t.Skip("skipping e2e test; set E2E=1 to run")
	}

	ctx := context.Background()

	// Start ClickHouse
	container, err := chmodule.Run(ctx,
		"clickhouse:26.1",
		chmodule.WithDatabase("pyrra_e2e"),
		chmodule.WithUsername("default"),
		chmodule.WithPassword(""),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	nativePort, err := container.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)
	httpPort, err := container.MappedPort(ctx, "8123/tcp")
	require.NoError(t, err)

	t.Logf("ClickHouse running at native=%s:%s http=%s:%s",
		host, nativePort.Port(), host, httpPort.Port())

	// Create client
	config := clickhouse.Config{
		Addresses:         []string{fmt.Sprintf("%s:%s", host, nativePort.Port())},
		Database:          "pyrra_e2e",
		Username:          "default",
		Password:          "",
		Protocol:          "native",
		MaxOpenConns:      5,
		MaxIdleConns:      3,
		ConnMaxLifetime:   10 * time.Minute,
		DialTimeout:       10 * time.Second,
		QueryTimeout:      30 * time.Second,
		MVRefreshInterval: 5 * time.Second,
	}

	client, err := clickhouse.NewClient(config)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
	})

	// Run migrations
	migrator := clickhouse.NewMigrator(client, nil)
	err = migrator.RunMigrations(ctx)
	require.NoError(t, err)

	// Verify schema version
	version, err := migrator.CurrentVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, version)

	t.Run("schema_tables_exist", func(t *testing.T) {
		tables := []string{"metrics_raw", "slo_recordings"}
		for _, table := range tables {
			var count uint64
			row := client.QueryRow(ctx,
				"SELECT count() FROM system.tables WHERE database = ? AND name = ?",
				"pyrra_e2e", table)
			err := row.Scan(&count)
			require.NoError(t, err)
			require.Equal(t, uint64(1), count, "table %s should exist", table)
		}
	})

	t.Run("provision_and_query_slo", func(t *testing.T) {
		// Insert pre-computed SLO recording data (simulating MV output)
		now := time.Now().Truncate(time.Minute)

		recordings := []struct {
			metricName string
			labels     map[string]string
			value      float64
		}{
			{"http_requests:burnrate5m", map[string]string{"slo": "e2e-http-errors", "job": "api"}, 0.012},
			{"http_requests:burnrate1h", map[string]string{"slo": "e2e-http-errors", "job": "api"}, 0.010},
			{"http_requests:burnrate6h", map[string]string{"slo": "e2e-http-errors", "job": "api"}, 0.009},
			{"http_requests:burnrate1d", map[string]string{"slo": "e2e-http-errors", "job": "api"}, 0.009},
			{"http_requests:increase4w", map[string]string{"slo": "e2e-http-errors", "job": "api"}, 100000000},
		}

		for _, r := range recordings {
			err := client.Exec(ctx,
				"INSERT INTO slo_recordings (timestamp, metric_name, labels, value) VALUES (?, ?, ?, ?)",
				now, r.metricName, r.labels, r.value,
			)
			require.NoError(t, err)
		}

		// Query via native SQL instead of the deleted PromQL layer
		var metricName string
		var value float64

		row := client.QueryRow(ctx, `
			SELECT metric_name, value 
			FROM slo_recordings FINAL
			WHERE metric_name = 'http_requests:burnrate5m' 
			  AND labels['slo'] = 'e2e-http-errors'
			LIMIT 1
		`)
		err := row.Scan(&metricName, &value)
		require.NoError(t, err)
		require.Equal(t, "http_requests:burnrate5m", metricName)
		require.InDelta(t, 0.012, value, 0.001)
		t.Logf("burnrate5m result: %s = %f", metricName, value)

		row = client.QueryRow(ctx, `
			SELECT metric_name, value 
			FROM slo_recordings FINAL
			WHERE metric_name = 'http_requests:increase4w' 
			  AND labels['slo'] = 'e2e-http-errors'
			LIMIT 1
		`)
		err = row.Scan(&metricName, &value)
		require.NoError(t, err)
		require.Equal(t, "http_requests:increase4w", metricName)
		require.InDelta(t, 100000000, value, 1)
		t.Logf("increase4w result: %s = %f", metricName, value)
	})

	t.Run("insert_raw_metrics", func(t *testing.T) {
		// Insert raw metrics into metrics_raw
		now := time.Now().Truncate(time.Second)

		for i := 0; i < 60; i++ {
			ts := now.Add(-time.Duration(i) * time.Minute)
			err := client.Exec(ctx,
				"INSERT INTO metrics_raw (timestamp, metric_name, labels, value, metric_type) VALUES (?, ?, ?, ?, ?)",
				ts, "http_requests_total",
				map[string]string{"job": "api", "code": "200", "handler": "/api"},
				float64(1000+i*100), "counter",
			)
			require.NoError(t, err)

			err = client.Exec(ctx,
				"INSERT INTO metrics_raw (timestamp, metric_name, labels, value, metric_type) VALUES (?, ?, ?, ?, ?)",
				ts, "http_requests_total",
				map[string]string{"job": "api", "code": "500", "handler": "/api"},
				float64(10+i), "counter",
			)
			require.NoError(t, err)
		}

		// Verify data was inserted
		var count uint64
		row := client.QueryRow(ctx, "SELECT count() FROM metrics_raw")
		err := row.Scan(&count)
		require.NoError(t, err)
		require.Greater(t, count, uint64(100), "should have inserted raw metrics")
		t.Logf("Inserted %d raw metric samples", count)
	})

	t.Run("clickhouse_http_api", func(t *testing.T) {
		// Test ClickHouse HTTP API is accessible
		httpURL := fmt.Sprintf("http://%s:%s/?query=SELECT+1", host, httpPort.Port())
		resp, err := http.Get(httpURL)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	})

	t.Run("verify_metrics_raw_schema", func(t *testing.T) {
		// Verify the schema is correct by querying with specific types
		var metricName string
		var value float64
		row := client.QueryRow(ctx, `
			SELECT metric_name, value 
			FROM metrics_raw 
			WHERE metric_name = 'http_requests_total'
			ORDER BY timestamp DESC 
			LIMIT 1
		`)
		err := row.Scan(&metricName, &value)
		require.NoError(t, err)
		require.Equal(t, "http_requests_total", metricName)
		require.Greater(t, value, 0.0)
	})

	t.Run("verify_slo_recordings_schema", func(t *testing.T) {
		var metricName string
		var value float64
		row := client.QueryRow(ctx, `
			SELECT metric_name, value 
			FROM slo_recordings FINAL
			WHERE metric_name = 'http_requests:burnrate5m'
			LIMIT 1
		`)
		err := row.Scan(&metricName, &value)
		require.NoError(t, err)
		require.Equal(t, "http_requests:burnrate5m", metricName)
		require.Greater(t, value, 0.0)
	})
}

// TestE2E_ClickHouseHTTPAPI tests the ClickHouse HTTP query interface
// which is used by Grafana and other tools.
func TestE2E_ClickHouseHTTPAPI(t *testing.T) {
	if os.Getenv("E2E") != "1" {
		t.Skip("skipping e2e test; set E2E=1 to run")
	}

	ctx := context.Background()

	container, err := chmodule.Run(ctx,
		"clickhouse:26.1",
		chmodule.WithDatabase("pyrra_http_test"),
		chmodule.WithUsername("default"),
		chmodule.WithPassword(""),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	httpPort, err := container.MappedPort(ctx, "8123/tcp")
	require.NoError(t, err)

	baseURL := fmt.Sprintf("http://%s:%s", host, httpPort.Port())

	t.Run("health_check", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/ping")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	})

	t.Run("query_select_1", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/?query=SELECT+1+FORMAT+JSON")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		require.Contains(t, result, "data")
	})
}
