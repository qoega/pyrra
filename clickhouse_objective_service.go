package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/bufbuild/connect-go"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/pyrra-dev/pyrra/clickhouse"
	objectivesv1alpha1 "github.com/pyrra-dev/pyrra/proto/objectives/v1alpha1"
	"github.com/pyrra-dev/pyrra/slo"
)

// clickhouseObjectiveService implements ObjectiveServiceHandler with native SQL queries
// against ClickHouse's slo_recordings table, bypassing PromQL entirely.
type clickhouseObjectiveService struct {
	logger     log.Logger
	client     *clickhouse.Client
	objectives *Objectives
}

// getObjective finds a single matching objective by expr.
func (s *clickhouseObjectiveService) getObjective(expr string) (slo.Objective, error) {
	matchers, err := parser.ParseMetricSelector(expr)
	if err != nil {
		return slo.Objective{}, connect.NewError(connect.CodeInvalidArgument, err)
	}

	matching := s.objectives.Match(matchers)
	if len(matching) == 0 {
		return slo.Objective{}, connect.NewError(connect.CodeNotFound, fmt.Errorf("no objective found for expr: %s", expr))
	}
	if len(matching) > 1 {
		return slo.Objective{}, connect.NewError(connect.CodeAborted, fmt.Errorf("expr matches more than one SLO, it matches: %d", len(matching)))
	}

	return matching[0], nil
}

// parseGroupingConditions parses the grouping string into SQL conditions.
func parseGroupingConditions(grouping string) (string, []*labels.Matcher) {
	if grouping == "" || grouping == "{}" {
		return "", nil
	}
	matchers, err := parser.ParseMetricSelector(grouping)
	if err != nil {
		return "", nil
	}
	return clickhouse.BuildLabelConditions(matchers), matchers
}

func (s *clickhouseObjectiveService) List(_ context.Context, req *connect.Request[objectivesv1alpha1.ListRequest]) (*connect.Response[objectivesv1alpha1.ListResponse], error) {
	var matchers []*labels.Matcher
	if expr := req.Msg.Expr; expr != "" {
		var err error
		matchers, err = parser.ParseMetricSelector(expr)
		if err != nil {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("failed to parse expr: %w", err))
		}
	}

	matchingObjectives := s.objectives.Match(matchers)
	objectives := make([]*objectivesv1alpha1.Objective, 0, len(matchingObjectives))
	for _, o := range matchingObjectives {
		protoObj := objectivesv1alpha1.FromInternal(o)
		objectives = append(objectives, protoObj)
	}

	if len(objectives) == 0 {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("no objectives found"))
	}

	return connect.NewResponse(&objectivesv1alpha1.ListResponse{
		Objectives: objectives,
	}), nil
}

func (s *clickhouseObjectiveService) GetStatus(ctx context.Context, req *connect.Request[objectivesv1alpha1.GetStatusRequest]) (*connect.Response[objectivesv1alpha1.GetStatusResponse], error) {
	objective, err := s.getObjective(req.Msg.Expr)
	if err != nil {
		return nil, err
	}

	extraConditions, _ := parseGroupingConditions(req.Msg.Grouping)

	sloName := objective.Name()
	totalMetric := clickhouse.TotalMetricName(objective)
	statuses := map[model.Fingerprint]*objectivesv1alpha1.ObjectiveStatus{}

	// Query total
	totalValues, err := clickhouse.QueryLatestRecordings(ctx, s.client, s.logger, totalMetric, sloName, extraConditions)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to query total", "metric", totalMetric, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	for fp, v := range totalValues {
		statuses[fp] = &objectivesv1alpha1.ObjectiveStatus{
			Labels: v.Labels,
			Availability: &objectivesv1alpha1.Availability{
				Percentage: 1,
				Total:      v.Value,
			},
		}
	}

	// Query errors based on indicator type
	switch objective.IndicatorType() {
	case slo.Ratio:
		errorMetric := clickhouse.ErrorMetricName(objective)
		errorValues, err := clickhouse.QueryLatestRecordings(ctx, s.client, s.logger, errorMetric, sloName, extraConditions)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to query errors", "metric", errorMetric, "err", err)
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		for fp, v := range errorValues {
			if status, exists := statuses[fp]; exists {
				status.Availability.Errors = v.Value
				status.Availability.Percentage = 1 - (v.Value / status.Availability.Total)
			}
		}

	case slo.Latency:
		// Errors = total - success
		successMetric := clickhouse.SuccessMetricName(objective)
		successValues, err := clickhouse.QueryLatestRecordings(ctx, s.client, s.logger, successMetric, sloName, extraConditions)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to query success", "metric", successMetric, "err", err)
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		for fp, sv := range successValues {
			if status, exists := statuses[fp]; exists {
				status.Availability.Errors = status.Availability.Total - sv.Value
				if status.Availability.Total > 0 {
					status.Availability.Percentage = 1 - (status.Availability.Errors / status.Availability.Total)
				}
			}
		}

	case slo.BoolGauge:
		// Errors = count - sum
		sumMetric := clickhouse.SuccessMetricName(objective)
		sumValues, err := clickhouse.QueryLatestRecordings(ctx, s.client, s.logger, sumMetric, sloName, extraConditions)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to query sum", "metric", sumMetric, "err", err)
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		for fp, sv := range sumValues {
			if status, exists := statuses[fp]; exists {
				status.Availability.Errors = status.Availability.Total - sv.Value
				if status.Availability.Total > 0 {
					status.Availability.Percentage = 1 - (status.Availability.Errors / status.Availability.Total)
				}
			}
		}
	}

	// Compute budget and filter
	statusSlice := make([]*objectivesv1alpha1.ObjectiveStatus, 0, len(statuses))
	for _, status := range statuses {
		status.Budget = &objectivesv1alpha1.Budget{}
		status.Budget.Total = 1 - objective.Target
		status.Budget.Remaining = (status.Budget.Total - (status.Availability.Errors / status.Availability.Total)) / status.Budget.Total
		status.Budget.Max = status.Budget.Total * status.Availability.Total

		if status.Availability.Total == 0 {
			continue
		}
		if math.IsNaN(status.Availability.Percentage) {
			status.Availability.Percentage = 1
		}
		if math.IsNaN(status.Budget.Remaining) {
			status.Budget.Remaining = 1
		}

		statusSlice = append(statusSlice, status)
	}

	return connect.NewResponse(&objectivesv1alpha1.GetStatusResponse{
		Status: statusSlice,
	}), nil
}

func (s *clickhouseObjectiveService) GetAlerts(ctx context.Context, req *connect.Request[objectivesv1alpha1.GetAlertsRequest]) (*connect.Response[objectivesv1alpha1.GetAlertsResponse], error) {
	var matchers []*labels.Matcher
	if expr := req.Msg.Expr; expr != "" {
		var err error
		matchers, err = parser.ParseMetricSelector(expr)
		if err != nil {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("failed to parse expr: %w", err))
		}
	}

	matchingObjectives := s.objectives.Match(matchers)
	if len(matchingObjectives) == 0 {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("no objectives found"))
	}

	extraConditions, groupingMatchers := parseGroupingConditions(req.Msg.Grouping)

	alerts := make([]*objectivesv1alpha1.Alert, 0)

	for _, objective := range matchingObjectives {
		if len(objective.Labels) == 0 {
			continue
		}

		sloName := objective.Name()
		windows := objective.Windows()

		// Build base labels from objective
		lset := map[string]string{}
		for _, l := range objective.Labels {
			lset[l.Name] = l.Value
		}

		// Query all burnrate windows
		burnrateValues, err := clickhouse.QueryBurnrateLatest(ctx, s.client, s.logger, objective, extraConditions)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to query burnrates", "slo", sloName, "err", err)
		}

		for _, w := range windows {
			queryShort, _ := objective.QueryBurnrate(w.Short, groupingMatchers)
			queryLong, _ := objective.QueryBurnrate(w.Long, groupingMatchers)

			// Determine alert state from burnrate values
			state := objectivesv1alpha1.Alert_inactive
			threshold := w.Factor * (1 - objective.Target)

			shortCurrent := -1.0
			longCurrent := -1.0

			if shortValues, ok := burnrateValues[w.Short]; ok {
				// For now, take the first (or only) series
				for _, sv := range shortValues {
					shortCurrent = sv.Value
					break
				}
			}
			if longValues, ok := burnrateValues[w.Long]; ok {
				for _, lv := range longValues {
					longCurrent = lv.Value
					break
				}
			}

			// Alert is firing if BOTH short AND long burnrates exceed threshold
			if shortCurrent >= 0 && longCurrent >= 0 &&
				shortCurrent > threshold && longCurrent > threshold {
				state = objectivesv1alpha1.Alert_firing
			} else if shortCurrent >= 0 || longCurrent >= 0 {
				// If at least one has data, mark as pending if either exceeds
				if (shortCurrent >= 0 && shortCurrent > threshold) ||
					(longCurrent >= 0 && longCurrent > threshold) {
					state = objectivesv1alpha1.Alert_pending
				}
			}

			if !req.Msg.Inactive && state == objectivesv1alpha1.Alert_inactive {
				continue
			}

			alert := &objectivesv1alpha1.Alert{
				Labels:   lset,
				Severity: string(w.Severity),
				For:      durationpb.New(w.For),
				Factor:   w.Factor,
				Short: &objectivesv1alpha1.Burnrate{
					Window:  durationpb.New(w.Short),
					Current: shortCurrent,
					Query:   queryShort,
				},
				Long: &objectivesv1alpha1.Burnrate{
					Window:  durationpb.New(w.Long),
					Current: longCurrent,
					Query:   queryLong,
				},
				State: state,
			}

			alerts = append(alerts, alert)
		}
	}

	return connect.NewResponse(&objectivesv1alpha1.GetAlertsResponse{Alerts: alerts}), nil
}

func (s *clickhouseObjectiveService) GraphErrorBudget(ctx context.Context, req *connect.Request[objectivesv1alpha1.GraphErrorBudgetRequest]) (*connect.Response[objectivesv1alpha1.GraphErrorBudgetResponse], error) {
	objective, err := s.getObjective(req.Msg.Expr)
	if err != nil {
		return nil, err
	}

	extraConditions, _ := parseGroupingConditions(req.Msg.Grouping)

	end := time.Now()
	start := end.Add(-1 * time.Hour)
	if !req.Msg.Start.AsTime().IsZero() && !req.Msg.End.AsTime().IsZero() {
		start = req.Msg.Start.AsTime()
		end = req.Msg.End.AsTime()
	}

	sloName := objective.Name()
	totalMetric := clickhouse.TotalMetricName(objective)

	// Query total timeseries
	totalSeries, err := clickhouse.QueryRecordingRange(ctx, s.client, s.logger, totalMetric, sloName, extraConditions, start, end)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to query total range", "metric", totalMetric, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Query error timeseries
	var errorSeries map[model.Fingerprint]*clickhouse.LabeledTimeseries
	switch objective.IndicatorType() {
	case slo.Ratio:
		errorMetric := clickhouse.ErrorMetricName(objective)
		errorSeries, err = clickhouse.QueryRecordingRange(ctx, s.client, s.logger, errorMetric, sloName, extraConditions, start, end)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to query error range", "metric", errorMetric, "err", err)
			return nil, connect.NewError(connect.CodeInternal, err)
		}

	case slo.Latency:
		// errors = total - success
		successMetric := clickhouse.SuccessMetricName(objective)
		successSeries, err := clickhouse.QueryRecordingRange(ctx, s.client, s.logger, successMetric, sloName, extraConditions, start, end)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to query success range", "err", err)
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		errorSeries = subtractTimeseries(totalSeries, successSeries)

	case slo.BoolGauge:
		// errors = count - sum
		sumMetric := clickhouse.SuccessMetricName(objective)
		sumSeries, err := clickhouse.QueryRecordingRange(ctx, s.client, s.logger, sumMetric, sloName, extraConditions, start, end)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to query sum range", "err", err)
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		errorSeries = subtractTimeseries(totalSeries, sumSeries)
	}

	// Compute error budget
	budgetSeries := clickhouse.ComputeErrorBudget(totalSeries, errorSeries, objective.Target)

	if len(budgetSeries) == 0 {
		level.Debug(s.logger).Log("msg", "no data for error budget")
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}

	// Convert to proto response
	return connect.NewResponse(&objectivesv1alpha1.GraphErrorBudgetResponse{
		Timeseries: labeledTimeseriesToProto(budgetSeries, objective.QueryErrorBudget()),
	}), nil
}

func (s *clickhouseObjectiveService) GraphRate(ctx context.Context, req *connect.Request[objectivesv1alpha1.GraphRateRequest]) (*connect.Response[objectivesv1alpha1.GraphRateResponse], error) {
	objective, err := s.getObjective(req.Msg.Expr)
	if err != nil {
		return nil, err
	}

	extraConditions, _ := parseGroupingConditions(req.Msg.Grouping)

	end := time.Now()
	start := end.Add(-1 * time.Hour)
	if !req.Msg.Start.AsTime().IsZero() && !req.Msg.End.AsTime().IsZero() {
		start = req.Msg.Start.AsTime()
		end = req.Msg.End.AsTime()
	}

	sloName := objective.Name()

	// Use the shortest burnrate window as rate proxy (5m burnrate approximates request rate)
	// The increase metrics don't represent rate directly, but burnrate does
	windows := objective.Windows()
	if len(windows) == 0 {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("no windows for objective"))
	}
	// Use the shortest short window
	shortestWindow := windows[0].Short
	for _, w := range windows {
		if w.Short < shortestWindow {
			shortestWindow = w.Short
		}
	}

	// Query total increase metric for the SLO window (gives total request count over the window)
	totalMetric := clickhouse.TotalMetricName(objective)
	totalSeries, err := clickhouse.QueryRecordingRange(ctx, s.client, s.logger, totalMetric, sloName, extraConditions, start, end)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to query total range for rate", "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if len(totalSeries) == 0 {
		level.Debug(s.logger).Log("msg", "no data for rate graph")
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}

	timeRange := rangeInterval(start, end)
	query := objective.RequestRange(timeRange)

	return connect.NewResponse(&objectivesv1alpha1.GraphRateResponse{
		Timeseries: labeledTimeseriesToProto(totalSeries, query),
	}), nil
}

func (s *clickhouseObjectiveService) GraphErrors(ctx context.Context, req *connect.Request[objectivesv1alpha1.GraphErrorsRequest]) (*connect.Response[objectivesv1alpha1.GraphErrorsResponse], error) {
	objective, err := s.getObjective(req.Msg.Expr)
	if err != nil {
		return nil, err
	}

	extraConditions, _ := parseGroupingConditions(req.Msg.Grouping)

	end := time.Now()
	start := end.Add(-1 * time.Hour)
	if !req.Msg.Start.AsTime().IsZero() && !req.Msg.End.AsTime().IsZero() {
		start = req.Msg.Start.AsTime()
		end = req.Msg.End.AsTime()
	}

	sloName := objective.Name()

	// Use the shortest burnrate window for error rate (burnrate IS the error rate)
	windows := objective.Windows()
	if len(windows) == 0 {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("no windows for objective"))
	}
	shortestWindow := windows[0].Short
	for _, w := range windows {
		if w.Short < shortestWindow {
			shortestWindow = w.Short
		}
	}

	burnrateMetric := objective.BurnrateName(shortestWindow)
	burnrateSeries, err := clickhouse.QueryRecordingRange(ctx, s.client, s.logger, burnrateMetric, sloName, extraConditions, start, end)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to query burnrate range for errors", "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if len(burnrateSeries) == 0 {
		level.Debug(s.logger).Log("msg", "no data for errors graph")
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}

	timeRange := rangeInterval(start, end)
	query := objective.ErrorsRange(timeRange)

	return connect.NewResponse(&objectivesv1alpha1.GraphErrorsResponse{
		Timeseries: labeledTimeseriesToProto(burnrateSeries, query),
	}), nil
}

func (s *clickhouseObjectiveService) GraphBurnrate(ctx context.Context, req *connect.Request[objectivesv1alpha1.GraphBurnrateRequest]) (*connect.Response[objectivesv1alpha1.GraphBurnrateResponse], error) {
	objective, err := s.getObjective(req.Msg.Expr)
	if err != nil {
		return nil, err
	}

	extraConditions, _ := parseGroupingConditions(req.Msg.Grouping)

	end := time.Now()
	start := end.Add(-1 * time.Hour)
	if !req.Msg.Start.AsTime().IsZero() && !req.Msg.End.AsTime().IsZero() {
		start = req.Msg.Start.AsTime()
		end = req.Msg.End.AsTime()
	}

	windows := objective.Windows()
	alertIndex := int(req.Msg.AlertIndex)
	if alertIndex < 0 || alertIndex >= len(windows) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("alert_index %d out of range [0, %d)", alertIndex, len(windows)))
	}
	w := windows[alertIndex]

	sloName := objective.Name()

	// Query short burnrate range
	shortMetric := objective.BurnrateName(w.Short)
	shortSeries, err := clickhouse.QueryRecordingRange(ctx, s.client, s.logger, shortMetric, sloName, extraConditions, start, end)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to query short burnrate range", "metric", shortMetric, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Query long burnrate range
	longMetric := objective.BurnrateName(w.Long)
	longSeries, err := clickhouse.QueryRecordingRange(ctx, s.client, s.logger, longMetric, sloName, extraConditions, start, end)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to query long burnrate range", "metric", longMetric, "err", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	shortQuery, _ := objective.QueryBurnrate(w.Short, nil)
	longQuery, _ := objective.QueryBurnrate(w.Long, nil)

	return connect.NewResponse(&objectivesv1alpha1.GraphBurnrateResponse{
		Short: labeledTimeseriesToProto(shortSeries, shortQuery),
		Long:  labeledTimeseriesToProto(longSeries, longQuery),
	}), nil
}

func (s *clickhouseObjectiveService) GraphDuration(_ context.Context, _ *connect.Request[objectivesv1alpha1.GraphDurationRequest]) (*connect.Response[objectivesv1alpha1.GraphDurationResponse], error) {
	// Duration graphs require raw histogram bucket data from metrics_raw,
	// which is not available in pre-computed slo_recordings.
	// Return empty response gracefully.
	return connect.NewResponse(&objectivesv1alpha1.GraphDurationResponse{
		Timeseries: nil,
	}), nil
}

// subtractTimeseries computes lhs - rhs for matching fingerprints and timestamps.
func subtractTimeseries(
	lhs map[model.Fingerprint]*clickhouse.LabeledTimeseries,
	rhs map[model.Fingerprint]*clickhouse.LabeledTimeseries,
) map[model.Fingerprint]*clickhouse.LabeledTimeseries {
	result := make(map[model.Fingerprint]*clickhouse.LabeledTimeseries)

	for fp, total := range lhs {
		sub, ok := rhs[fp]
		if !ok {
			// If no matching series, the entire total is errors
			result[fp] = total
			continue
		}

		// Build lookup by timestamp
		subByTS := make(map[int64]float64, len(sub.Values))
		for _, v := range sub.Values {
			subByTS[v.Timestamp.Unix()] = v.Value
		}

		diff := &clickhouse.LabeledTimeseries{
			Labels: total.Labels,
		}
		for _, tv := range total.Values {
			subVal := subByTS[tv.Timestamp.Unix()]
			diff.Values = append(diff.Values, clickhouse.TimestampedValue{
				Timestamp: tv.Timestamp,
				Value:     tv.Value - subVal,
			})
		}
		result[fp] = diff
	}

	return result
}

// labeledTimeseriesToProto converts a map of LabeledTimeseries to the proto Timeseries format
// used by the UI. The format matches matrixToValues: first series is timestamps, then one
// series per labeled timeseries.
func labeledTimeseriesToProto(
	seriesMap map[model.Fingerprint]*clickhouse.LabeledTimeseries,
	query string,
) *objectivesv1alpha1.Timeseries {
	if len(seriesMap) == 0 {
		return nil
	}

	// Sort fingerprints for deterministic output
	fps := make([]model.Fingerprint, 0, len(seriesMap))
	for fp := range seriesMap {
		fps = append(fps, fp)
	}
	sort.Slice(fps, func(i, j int) bool { return fps[i] < fps[j] })

	// For single series, use the simple format
	if len(fps) == 1 {
		ts := seriesMap[fps[0]]
		timestamps := make([]float64, len(ts.Values))
		values := make([]float64, len(ts.Values))
		for i, v := range ts.Values {
			timestamps[i] = float64(v.Timestamp.Unix())
			values[i] = v.Value
			if math.IsNaN(values[i]) {
				values[i] = 0
			}
		}

		lbls := make([]string, 1)
		m := model.Metric{}
		for k, v := range ts.Labels {
			m[model.LabelName(k)] = model.LabelValue(v)
		}
		lbls[0] = model.LabelSet(m).String()

		return &objectivesv1alpha1.Timeseries{
			Labels: lbls,
			Query:  query,
			Series: []*objectivesv1alpha1.Series{
				{Values: timestamps},
				{Values: values},
			},
		}
	}

	// Multiple series: align timestamps across all series
	// Collect all unique timestamps
	tsSet := make(map[int64]struct{})
	for _, fp := range fps {
		for _, v := range seriesMap[fp].Values {
			tsSet[v.Timestamp.Unix()] = struct{}{}
		}
	}

	allTimestamps := make([]int64, 0, len(tsSet))
	for t := range tsSet {
		allTimestamps = append(allTimestamps, t)
	}
	sort.Slice(allTimestamps, func(i, j int) bool { return allTimestamps[i] < allTimestamps[j] })

	// Build series arrays
	numSeries := len(fps)
	timestampSeries := make([]float64, len(allTimestamps))
	dataSeries := make([][]float64, numSeries)
	for i := range dataSeries {
		dataSeries[i] = make([]float64, len(allTimestamps))
	}

	for i, t := range allTimestamps {
		timestampSeries[i] = float64(t)
	}

	lbls := make([]string, numSeries)
	for si, fp := range fps {
		ts := seriesMap[fp]
		m := model.Metric{}
		for k, v := range ts.Labels {
			m[model.LabelName(k)] = model.LabelValue(v)
		}
		lbls[si] = model.LabelSet(m).String()

		// Build lookup by timestamp
		valByTS := make(map[int64]float64, len(ts.Values))
		for _, v := range ts.Values {
			valByTS[v.Timestamp.Unix()] = v.Value
		}

		for ti, t := range allTimestamps {
			if v, ok := valByTS[t]; ok && !math.IsNaN(v) {
				dataSeries[si][ti] = v
			}
		}
	}

	series := make([]*objectivesv1alpha1.Series, 0, numSeries+1)
	series = append(series, &objectivesv1alpha1.Series{Values: timestampSeries})
	for _, ds := range dataSeries {
		series = append(series, &objectivesv1alpha1.Series{Values: ds})
	}

	return &objectivesv1alpha1.Timeseries{
		Labels: lbls,
		Query:  query,
		Series: series,
	}
}
