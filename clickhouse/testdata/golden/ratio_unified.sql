CREATE MATERIALIZED VIEW slo_monitoring_http_errors_mv
REFRESH EVERY 30 SECOND APPEND
TO slo_recordings
AS
SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_requests:increase4w' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-errors'), map('job', 'thanos-receive-default')
    ) AS labels,
    sum(m.value) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(2419200)
  AND m.metric_name = 'http_requests_total'
  AND m.labels['job'] = 'thanos-receive-default'


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_requests:error_increase4w' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-errors'), map('job', 'thanos-receive-default')
    ) AS labels,
    sum(m.value) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(2419200)
  AND m.metric_name = 'http_requests_total'
  AND m.labels['job'] = 'thanos-receive-default' AND match(m.labels['code'], '5..')


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_requests:burnrate5m' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-errors'), map('job', 'thanos-receive-default')
    ) AS labels,
    sumIf(m.value, m.labels['job'] = 'thanos-receive-default' AND match(m.labels['code'], '5..')) / nullIf(sumIf(m.value, m.labels['job'] = 'thanos-receive-default'), 0) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(300)
  AND m.metric_name = 'http_requests_total'


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_requests:burnrate30m' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-errors'), map('job', 'thanos-receive-default')
    ) AS labels,
    sumIf(m.value, m.labels['job'] = 'thanos-receive-default' AND match(m.labels['code'], '5..')) / nullIf(sumIf(m.value, m.labels['job'] = 'thanos-receive-default'), 0) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(1800)
  AND m.metric_name = 'http_requests_total'


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_requests:burnrate1h' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-errors'), map('job', 'thanos-receive-default')
    ) AS labels,
    sumIf(m.value, m.labels['job'] = 'thanos-receive-default' AND match(m.labels['code'], '5..')) / nullIf(sumIf(m.value, m.labels['job'] = 'thanos-receive-default'), 0) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(3600)
  AND m.metric_name = 'http_requests_total'


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_requests:burnrate2h' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-errors'), map('job', 'thanos-receive-default')
    ) AS labels,
    sumIf(m.value, m.labels['job'] = 'thanos-receive-default' AND match(m.labels['code'], '5..')) / nullIf(sumIf(m.value, m.labels['job'] = 'thanos-receive-default'), 0) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(7200)
  AND m.metric_name = 'http_requests_total'


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_requests:burnrate6h' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-errors'), map('job', 'thanos-receive-default')
    ) AS labels,
    sumIf(m.value, m.labels['job'] = 'thanos-receive-default' AND match(m.labels['code'], '5..')) / nullIf(sumIf(m.value, m.labels['job'] = 'thanos-receive-default'), 0) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(21600)
  AND m.metric_name = 'http_requests_total'


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_requests:burnrate1d' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-errors'), map('job', 'thanos-receive-default')
    ) AS labels,
    sumIf(m.value, m.labels['job'] = 'thanos-receive-default' AND match(m.labels['code'], '5..')) / nullIf(sumIf(m.value, m.labels['job'] = 'thanos-receive-default'), 0) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(86400)
  AND m.metric_name = 'http_requests_total'


UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'http_requests:burnrate4d' AS metric_name,
    mapConcat(
        map(),
        map('slo', 'monitoring-http-errors'), map('job', 'thanos-receive-default')
    ) AS labels,
    sumIf(m.value, m.labels['job'] = 'thanos-receive-default' AND match(m.labels['code'], '5..')) / nullIf(sumIf(m.value, m.labels['job'] = 'thanos-receive-default'), 0) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(345600)
  AND m.metric_name = 'http_requests_total'
