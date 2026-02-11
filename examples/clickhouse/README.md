# ClickHouse Backend for Pyrra

This directory contains examples for running Pyrra with ClickHouse as the metrics backend instead of Prometheus.

## Overview

The ClickHouse backend stores SLO metrics in ClickHouse using Refreshable Materialized Views (RMVs) instead of Prometheus recording rules. This provides:

- **Scalable storage**: ClickHouse can handle petabytes of metrics data
- **Fast aggregations**: Columnar storage optimized for analytical queries
- **Flexible retention**: Fine-grained TTL control per table
- **SQL queries**: Use standard SQL for custom analysis

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Metrics       │────▶│    ClickHouse    │◀────│     Pyrra       │
│   Ingestion     │     │                  │     │   (ClickHouse   │
│  (Remote Write) │     │  ┌────────────┐  │     │    Backend)     │
└─────────────────┘     │  │metrics_raw │  │     └─────────────────┘
                        │  └────────────┘  │              │
                        │        │         │              │
                        │        ▼         │              ▼
                        │  ┌────────────┐  │     ┌─────────────────┐
                        │  │   MVs      │  │     │   Pyrra API     │
                        │  │(Generated) │  │     │   (Frontend)    │
                        │  └────────────┘  │     └─────────────────┘
                        │        │         │
                        │        ▼         │
                        │  ┌────────────┐  │
                        │  │slo_        │  │
                        │  │recordings  │  │
                        │  └────────────┘  │
                        └──────────────────┘
```

## Quick Start

### 1. Start ClickHouse

```bash
docker-compose up -d clickhouse
```

### 2. Start Pyrra with ClickHouse Backend

```bash
docker-compose up -d pyrra-clickhouse
```

### 3. Start Pyrra API/UI

```bash
docker-compose up -d pyrra-api
```

### 4. Access the UI

Open http://localhost:9099 in your browser.

## Configuration

### SLO Definition

SLO definitions are the same YAML format used with Prometheus:

```yaml
apiVersion: pyrra.dev/v1alpha1
kind: ServiceLevelObjective
metadata:
  name: http-errors
  namespace: monitoring
  labels:
    prometheus: k8s
    role: alert-rules
    pyrra.dev/team: platform
spec:
  target: "99"
  window: 4w
  indicator:
    ratio:
      errors:
        metric: http_requests_total{job="api-server",code=~"5.."}
      total:
        metric: http_requests_total{job="api-server"}
      grouping:
        - handler
```

### ClickHouse Backend CLI Options

```
pyrra clickhouse [flags]

Flags:
      --addresses strings          ClickHouse server addresses (default [localhost:9000])
      --config-files string        Config files glob pattern (default "/etc/pyrra/*.yaml")
      --database string            ClickHouse database name (default "pyrra")
      --generic-rules              Enable generic recording rules
      --mv-refresh-interval duration   MV refresh interval (default 30s)
      --password string            ClickHouse password
      --tls-ca-file string         CA certificate file for TLS
      --tls-cert-file string       Client certificate file for TLS
      --tls-enabled                Enable TLS connection
      --tls-key-file string        Client key file for TLS
      --tls-skip-verify            Skip TLS certificate verification
      --username string            ClickHouse username (default "default")
```

## Metrics Ingestion

ClickHouse needs to receive metrics in a compatible format. Options include:

### Option 1: Prometheus Remote Write (Recommended)

Use a remote write adapter like [ClickHouse's native support](https://clickhouse.com/docs/en/integrations/prometheus) or [Vector](https://vector.dev/).

Example Vector configuration:

```toml
[sources.prometheus]
type = "prometheus_remote_write"
address = "0.0.0.0:9201"

[sinks.clickhouse]
type = "clickhouse"
inputs = ["prometheus"]
endpoint = "http://clickhouse:8123"
database = "pyrra"
table = "metrics_raw"
```

### Option 2: Direct Scraping

Use [clickhouse-exporter](https://github.com/ClickHouse/clickhouse_exporter) or similar tools to scrape Prometheus endpoints and write to ClickHouse.

### Required Schema

The `metrics_raw` table schema (created automatically by migrations):

```sql
CREATE TABLE metrics_raw (
    timestamp DateTime64(3),
    metric_name LowCardinality(String),
    labels Map(String, String),
    value Float64,
    metric_type Enum8('counter' = 1, 'gauge' = 2, 'histogram' = 3, 'summary' = 4, 'unknown' = 0) DEFAULT 'unknown',
    _date Date MATERIALIZED toDate(timestamp),
    _labels_hash UInt64 MATERIALIZED xxHash64(toString(labels))
) ENGINE = MergeTree()
PARTITION BY _date
ORDER BY (metric_name, _labels_hash, timestamp)
TTL timestamp + INTERVAL 35 DAY
```

## How It Works

### Materialized Views

When Pyrra processes an SLO definition, it creates Materialized Views that:

1. **Read from `metrics_raw`**: Filter metrics matching the SLO's metric selectors
2. **Compute aggregations**: Calculate burn rates, error rates, and totals
3. **Write to `slo_recordings`**: Store synthetic metrics similar to Prometheus recording rules

Example generated MV for a ratio SLO:

```sql
CREATE MATERIALIZED VIEW slo_burnrate_http_errors_5m_mv
REFRESH EVERY 30 SECOND APPEND
TO slo_recordings
AS
SELECT 
    toStartOfMinute(now()) AS timestamp,
    'http_requests:burnrate5m' AS metric_name,
    mapConcat(
        map('handler', labels['handler']),
        map('slo', 'http-errors', 'job', 'api-server')
    ) AS labels,
    sumIf(value, labels['job'] = 'api-server' AND match(labels['code'], '5..')) 
      / nullIf(sumIf(value, labels['job'] = 'api-server'), 0) AS value
FROM metrics_raw
WHERE timestamp >= now() - toIntervalSecond(300)
  AND metric_name = 'http_requests_total'
GROUP BY labels['handler']
```

### Query Flow

When the Pyrra API queries for SLO status:

1. The ObjectiveService receives an RPC call (e.g., GetStatus, GraphErrorBudget)
2. It runs native SQL queries against `slo_recordings`
3. Results are returned as protobuf messages to the UI

## Comparison with Prometheus Backend

| Feature | Prometheus | ClickHouse |
|---------|------------|------------|
| Storage | Time-series DB | Columnar OLAP |
| Query Language | PromQL | Native SQL |
| Retention | Global or per-metric | Per-table TTL |
| Scalability | Limited | Horizontal |
| Recording Rules | Prometheus rules | Materialized Views |
| Alerting | Prometheus Alertmanager | External (future) |

## Limitations

1. **No native alerting**: ClickHouse doesn't have built-in alerting. Use external tools.
2. **Delayed aggregations**: MVs refresh periodically (default 30s), not real-time.

## Troubleshooting

### Check MV Status

```sql
SELECT name, last_refresh_time, next_refresh_time, status
FROM system.view_refreshes
WHERE database = 'pyrra';
```

### Verify Data in slo_recordings

```sql
SELECT metric_name, labels, value, timestamp
FROM slo_recordings
WHERE timestamp > now() - INTERVAL 5 MINUTE
ORDER BY timestamp DESC
LIMIT 10;
```

### Check for Errors

```sql
SELECT *
FROM system.query_log
WHERE type = 'ExceptionWhileProcessing'
  AND query LIKE '%slo_%'
ORDER BY event_time DESC
LIMIT 10;
```

## Files in This Directory

- `docker-compose.yaml` - Complete setup with ClickHouse, Pyrra, and Grafana
- `slos/` - Example SLO definitions
- `clickhouse/` - ClickHouse configuration files



