CREATE MATERIALIZED VIEW slo_monitoring_probe_success_mv
REFRESH EVERY 30 SECOND APPEND
TO slo_recordings
AS
SELECT
    toStartOfMinute(now()) AS timestamp,
    'probe_success:count4w' AS metric_name,
    mapConcat(
        map('instance', m.labels['instance']),
        map('slo', 'monitoring-probe-success'), map('job', 'blackbox')
    ) AS labels,
    toFloat64(count()) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(2419200)
  AND m.metric_name = 'probe_success'
  AND m.labels['job'] = 'blackbox'
GROUP BY m.labels['instance']

UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'probe_success:sum4w' AS metric_name,
    mapConcat(
        map('instance', m.labels['instance']),
        map('slo', 'monitoring-probe-success'), map('job', 'blackbox')
    ) AS labels,
    sum(m.value) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(2419200)
  AND m.metric_name = 'probe_success'
  AND m.labels['job'] = 'blackbox'
GROUP BY m.labels['instance']

UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'probe_success:burnrate5m' AS metric_name,
    mapConcat(
        map('instance', m.labels['instance']),
        map('slo', 'monitoring-probe-success'), map('job', 'blackbox')
    ) AS labels,
    1 - (sum(m.value) / nullIf(count(), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(300)
  AND m.metric_name = 'probe_success'
  AND m.labels['job'] = 'blackbox'
GROUP BY m.labels['instance']

UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'probe_success:burnrate30m' AS metric_name,
    mapConcat(
        map('instance', m.labels['instance']),
        map('slo', 'monitoring-probe-success'), map('job', 'blackbox')
    ) AS labels,
    1 - (sum(m.value) / nullIf(count(), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(1800)
  AND m.metric_name = 'probe_success'
  AND m.labels['job'] = 'blackbox'
GROUP BY m.labels['instance']

UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'probe_success:burnrate1h' AS metric_name,
    mapConcat(
        map('instance', m.labels['instance']),
        map('slo', 'monitoring-probe-success'), map('job', 'blackbox')
    ) AS labels,
    1 - (sum(m.value) / nullIf(count(), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(3600)
  AND m.metric_name = 'probe_success'
  AND m.labels['job'] = 'blackbox'
GROUP BY m.labels['instance']

UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'probe_success:burnrate2h' AS metric_name,
    mapConcat(
        map('instance', m.labels['instance']),
        map('slo', 'monitoring-probe-success'), map('job', 'blackbox')
    ) AS labels,
    1 - (sum(m.value) / nullIf(count(), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(7200)
  AND m.metric_name = 'probe_success'
  AND m.labels['job'] = 'blackbox'
GROUP BY m.labels['instance']

UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'probe_success:burnrate6h' AS metric_name,
    mapConcat(
        map('instance', m.labels['instance']),
        map('slo', 'monitoring-probe-success'), map('job', 'blackbox')
    ) AS labels,
    1 - (sum(m.value) / nullIf(count(), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(21600)
  AND m.metric_name = 'probe_success'
  AND m.labels['job'] = 'blackbox'
GROUP BY m.labels['instance']

UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'probe_success:burnrate1d' AS metric_name,
    mapConcat(
        map('instance', m.labels['instance']),
        map('slo', 'monitoring-probe-success'), map('job', 'blackbox')
    ) AS labels,
    1 - (sum(m.value) / nullIf(count(), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(86400)
  AND m.metric_name = 'probe_success'
  AND m.labels['job'] = 'blackbox'
GROUP BY m.labels['instance']

UNION ALL

SELECT
    toStartOfMinute(now()) AS timestamp,
    'probe_success:burnrate4d' AS metric_name,
    mapConcat(
        map('instance', m.labels['instance']),
        map('slo', 'monitoring-probe-success'), map('job', 'blackbox')
    ) AS labels,
    1 - (sum(m.value) / nullIf(count(), 0)) AS value
FROM metrics_raw AS m
WHERE m.timestamp >= now() - toIntervalSecond(345600)
  AND m.metric_name = 'probe_success'
  AND m.labels['job'] = 'blackbox'
GROUP BY m.labels['instance']