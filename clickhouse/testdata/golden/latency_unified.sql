CREATE MATERIALIZED VIEW slo_monitoring_http_latency_mv
REFRESH EVERY 30 SECOND APPEND
TO slo_recordings
AS
SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_request_duration_seconds:increase4w' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-latency'), map('job', 'api')
    ) AS labels,
    sum(m.value) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(2419200)
  AND m.metric_name = 'http_request_duration_seconds_count'
  AND m.labels['job'] = 'api'


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_request_duration_seconds:success_increase4w' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-latency'), map('job', 'api', 'le', '1')
    ) AS labels,
    sum(m.value) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(2419200)
  AND m.metric_name = 'http_request_duration_seconds_bucket'
  AND m.labels['job'] = 'api' AND m.labels['le'] = '1'


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_request_duration_seconds:burnrate5m' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-latency'), map('job', 'api')
    ) AS labels,
    1 - (sumIf(m.value, m.metric_name = 'http_request_duration_seconds_bucket' AND m.labels['job'] = 'api' AND m.labels['le'] = '1') / nullIf(sumIf(m.value, m.metric_name = 'http_request_duration_seconds_count' AND m.labels['job'] = 'api'), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(300)
  AND m.metric_name IN ('http_request_duration_seconds_bucket', 'http_request_duration_seconds_count')


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_request_duration_seconds:burnrate30m' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-latency'), map('job', 'api')
    ) AS labels,
    1 - (sumIf(m.value, m.metric_name = 'http_request_duration_seconds_bucket' AND m.labels['job'] = 'api' AND m.labels['le'] = '1') / nullIf(sumIf(m.value, m.metric_name = 'http_request_duration_seconds_count' AND m.labels['job'] = 'api'), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(1800)
  AND m.metric_name IN ('http_request_duration_seconds_bucket', 'http_request_duration_seconds_count')


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_request_duration_seconds:burnrate1h' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-latency'), map('job', 'api')
    ) AS labels,
    1 - (sumIf(m.value, m.metric_name = 'http_request_duration_seconds_bucket' AND m.labels['job'] = 'api' AND m.labels['le'] = '1') / nullIf(sumIf(m.value, m.metric_name = 'http_request_duration_seconds_count' AND m.labels['job'] = 'api'), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(3600)
  AND m.metric_name IN ('http_request_duration_seconds_bucket', 'http_request_duration_seconds_count')


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_request_duration_seconds:burnrate2h' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-latency'), map('job', 'api')
    ) AS labels,
    1 - (sumIf(m.value, m.metric_name = 'http_request_duration_seconds_bucket' AND m.labels['job'] = 'api' AND m.labels['le'] = '1') / nullIf(sumIf(m.value, m.metric_name = 'http_request_duration_seconds_count' AND m.labels['job'] = 'api'), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(7200)
  AND m.metric_name IN ('http_request_duration_seconds_bucket', 'http_request_duration_seconds_count')


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_request_duration_seconds:burnrate6h' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-latency'), map('job', 'api')
    ) AS labels,
    1 - (sumIf(m.value, m.metric_name = 'http_request_duration_seconds_bucket' AND m.labels['job'] = 'api' AND m.labels['le'] = '1') / nullIf(sumIf(m.value, m.metric_name = 'http_request_duration_seconds_count' AND m.labels['job'] = 'api'), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(21600)
  AND m.metric_name IN ('http_request_duration_seconds_bucket', 'http_request_duration_seconds_count')


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_request_duration_seconds:burnrate1d' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-latency'), map('job', 'api')
    ) AS labels,
    1 - (sumIf(m.value, m.metric_name = 'http_request_duration_seconds_bucket' AND m.labels['job'] = 'api' AND m.labels['le'] = '1') / nullIf(sumIf(m.value, m.metric_name = 'http_request_duration_seconds_count' AND m.labels['job'] = 'api'), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(86400)
  AND m.metric_name IN ('http_request_duration_seconds_bucket', 'http_request_duration_seconds_count')


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_request_duration_seconds:burnrate4d' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-latency'), map('job', 'api')
    ) AS labels,
    1 - (sumIf(m.value, m.metric_name = 'http_request_duration_seconds_bucket' AND m.labels['job'] = 'api' AND m.labels['le'] = '1') / nullIf(sumIf(m.value, m.metric_name = 'http_request_duration_seconds_count' AND m.labels['job'] = 'api'), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(345600)
  AND m.metric_name IN ('http_request_duration_seconds_bucket', 'http_request_duration_seconds_count')
