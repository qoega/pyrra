package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pyrra-dev/pyrra/slo"
)

// Provisioner manages ClickHouse MVs based on SLO definitions
type Provisioner struct {
	client    *Client
	generator *MVGenerator
	logger    log.Logger

	mu         sync.Mutex
	activeMVs  map[string]bool // tracks which MVs are currently active
	objectiveMVs map[string][]string // maps objective name to its MV names
}

// NewProvisioner creates a new MV provisioner
func NewProvisioner(client *Client, config Config, logger log.Logger) *Provisioner {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Provisioner{
		client:       client,
		generator:    NewMVGenerator(config.MVRefreshInterval),
		logger:       logger,
		activeMVs:    make(map[string]bool),
		objectiveMVs: make(map[string][]string),
	}
}

// ProvisionObjective creates or updates MVs for a single objective
func (p *Provisioner) ProvisionObjective(ctx context.Context, obj slo.Objective) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	objName := obj.Name()
	if objName == "" {
		return fmt.Errorf("objective has no name")
	}

	level.Info(p.logger).Log("msg", "provisioning MVs for objective", "objective", objName)

	// Generate MVs for this objective
	mvs, err := p.generator.GenerateMVs(obj)
	if err != nil {
		return fmt.Errorf("generate MVs: %w", err)
	}

	// Drop old MVs for this objective if they exist
	if oldMVs, ok := p.objectiveMVs[objName]; ok {
		for _, mvName := range oldMVs {
			if err := p.dropMV(ctx, mvName); err != nil {
				level.Warn(p.logger).Log("msg", "failed to drop old MV", "mv", mvName, "err", err)
			}
			delete(p.activeMVs, mvName)
		}
	}

	// Create new MVs
	var newMVNames []string
	for _, mv := range mvs {
		if err := p.createMV(ctx, mv); err != nil {
			level.Error(p.logger).Log("msg", "failed to create MV", "mv", mv.Name, "err", err)
			return fmt.Errorf("create MV %s: %w", mv.Name, err)
		}
		p.activeMVs[mv.Name] = true
		newMVNames = append(newMVNames, mv.Name)
	}

	p.objectiveMVs[objName] = newMVNames
	level.Info(p.logger).Log("msg", "provisioned MVs for objective", "objective", objName, "count", len(mvs))

	return nil
}

// DeprovisionObjective removes all MVs for an objective
func (p *Provisioner) DeprovisionObjective(ctx context.Context, objName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	mvNames, ok := p.objectiveMVs[objName]
	if !ok {
		level.Debug(p.logger).Log("msg", "no MVs found for objective", "objective", objName)
		return nil
	}

	level.Info(p.logger).Log("msg", "deprovisioning MVs for objective", "objective", objName)

	for _, mvName := range mvNames {
		if err := p.dropMV(ctx, mvName); err != nil {
			level.Warn(p.logger).Log("msg", "failed to drop MV", "mv", mvName, "err", err)
			// Continue trying to drop other MVs
		}
		delete(p.activeMVs, mvName)
	}

	delete(p.objectiveMVs, objName)
	return nil
}

// ReconcileAll reconciles all objectives, creating/updating/deleting MVs as needed
func (p *Provisioner) ReconcileAll(ctx context.Context, objectives []slo.Objective) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	level.Info(p.logger).Log("msg", "reconciling all objectives", "count", len(objectives))

	// Build set of current objectives
	currentObjs := make(map[string]slo.Objective)
	for _, obj := range objectives {
		name := obj.Name()
		if name != "" {
			currentObjs[name] = obj
		}
	}

	// Find objectives to remove (exist in cache but not in current)
	var toRemove []string
	for objName := range p.objectiveMVs {
		if _, ok := currentObjs[objName]; !ok {
			toRemove = append(toRemove, objName)
		}
	}

	// Remove stale objectives
	for _, objName := range toRemove {
		level.Info(p.logger).Log("msg", "removing stale objective", "objective", objName)
		for _, mvName := range p.objectiveMVs[objName] {
			if err := p.dropMV(ctx, mvName); err != nil {
				level.Warn(p.logger).Log("msg", "failed to drop MV", "mv", mvName, "err", err)
			}
			delete(p.activeMVs, mvName)
		}
		delete(p.objectiveMVs, objName)
	}

	// Provision/update current objectives
	for _, obj := range objectives {
		objName := obj.Name()
		if objName == "" {
			continue
		}

		// Generate MVs for this objective
		mvs, err := p.generator.GenerateMVs(obj)
		if err != nil {
			level.Error(p.logger).Log("msg", "failed to generate MVs", "objective", objName, "err", err)
			continue
		}

		// Check if MVs need updating (for now, always recreate)
		// A more sophisticated approach would compare MV definitions
		needsUpdate := true
		if existingMVs, ok := p.objectiveMVs[objName]; ok {
			// Simple check: same number of MVs and same names
			if len(existingMVs) == len(mvs) {
				needsUpdate = false
				newNames := make(map[string]bool)
				for _, mv := range mvs {
					newNames[mv.Name] = true
				}
				for _, name := range existingMVs {
					if !newNames[name] {
						needsUpdate = true
						break
					}
				}
			}
		}

		if !needsUpdate {
			level.Debug(p.logger).Log("msg", "objective MVs up to date", "objective", objName)
			continue
		}

		// Drop old MVs
		if oldMVs, ok := p.objectiveMVs[objName]; ok {
			for _, mvName := range oldMVs {
				if err := p.dropMV(ctx, mvName); err != nil {
					level.Warn(p.logger).Log("msg", "failed to drop old MV", "mv", mvName, "err", err)
				}
				delete(p.activeMVs, mvName)
			}
		}

		// Create new MVs
		var newMVNames []string
		for _, mv := range mvs {
			if err := p.createMV(ctx, mv); err != nil {
				level.Error(p.logger).Log("msg", "failed to create MV", "mv", mv.Name, "err", err)
				// Continue trying other MVs
				continue
			}
			p.activeMVs[mv.Name] = true
			newMVNames = append(newMVNames, mv.Name)
		}
		p.objectiveMVs[objName] = newMVNames
	}

	level.Info(p.logger).Log("msg", "reconciliation complete", "objectives", len(currentObjs), "total_mvs", len(p.activeMVs))
	return nil
}

// SyncFromDatabase discovers existing MVs from ClickHouse and populates the cache
func (p *Provisioner) SyncFromDatabase(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	rows, err := p.client.Query(ctx, `
		SELECT name 
		FROM system.tables 
		WHERE database = currentDatabase() 
		  AND engine = 'MaterializedView'
		  AND name LIKE 'slo_%_mv'
	`)
	if err != nil {
		return fmt.Errorf("query MVs: %w", err)
	}
	defer rows.Close()

	p.activeMVs = make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan MV name: %w", err)
		}
		p.activeMVs[name] = true
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate MVs: %w", err)
	}

	level.Info(p.logger).Log("msg", "synced MVs from database", "count", len(p.activeMVs))
	return nil
}

// GetActiveMVs returns a copy of active MV names
func (p *Provisioner) GetActiveMVs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	mvs := make([]string, 0, len(p.activeMVs))
	for mv := range p.activeMVs {
		mvs = append(mvs, mv)
	}
	return mvs
}

// GetObjectiveMVs returns MV names for a specific objective
func (p *Provisioner) GetObjectiveMVs(objName string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if mvs, ok := p.objectiveMVs[objName]; ok {
		result := make([]string, len(mvs))
		copy(result, mvs)
		return result
	}
	return nil
}

// DropAllMVs removes all managed MVs (use for cleanup/reset)
func (p *Provisioner) DropAllMVs(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	level.Info(p.logger).Log("msg", "dropping all managed MVs")

	var errs []error
	for mvName := range p.activeMVs {
		if err := p.dropMV(ctx, mvName); err != nil {
			level.Warn(p.logger).Log("msg", "failed to drop MV", "mv", mvName, "err", err)
			errs = append(errs, err)
		}
	}

	p.activeMVs = make(map[string]bool)
	p.objectiveMVs = make(map[string][]string)

	if len(errs) > 0 {
		return fmt.Errorf("failed to drop %d MVs", len(errs))
	}
	return nil
}

// createMV creates a materialized view in ClickHouse
func (p *Provisioner) createMV(ctx context.Context, mv GeneratedMV) error {
	level.Debug(p.logger).Log("msg", "creating MV", "name", mv.Name)

	// First drop if exists to ensure clean state
	if err := p.dropMV(ctx, mv.Name); err != nil {
		// Ignore errors, MV may not exist
		level.Debug(p.logger).Log("msg", "drop before create failed (may not exist)", "name", mv.Name, "err", err)
	}

	if err := p.client.Exec(ctx, mv.SQL); err != nil {
		return fmt.Errorf("execute CREATE: %w", err)
	}

	return nil
}

// dropMV drops a materialized view from ClickHouse
func (p *Provisioner) dropMV(ctx context.Context, name string) error {
	level.Debug(p.logger).Log("msg", "dropping MV", "name", name)

	sql := fmt.Sprintf("DROP VIEW IF EXISTS %s", name)
	if err := p.client.Exec(ctx, sql); err != nil {
		return fmt.Errorf("execute DROP: %w", err)
	}
	return nil
}

// WatchObjectives starts a goroutine that periodically reconciles objectives
func (p *Provisioner) WatchObjectives(ctx context.Context, interval time.Duration, getObjectives func() []slo.Objective) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			level.Info(p.logger).Log("msg", "stopping objective watcher")
			return
		case <-ticker.C:
			objectives := getObjectives()
			if err := p.ReconcileAll(ctx, objectives); err != nil {
				level.Error(p.logger).Log("msg", "failed to reconcile objectives", "err", err)
			}
		}
	}
}

// MVStatus represents the status of an MV
type MVStatus struct {
	Name         string
	Exists       bool
	LastRefresh  time.Time
	NextRefresh  time.Time
	RowCount     uint64
	Error        string
}

// GetMVStatus returns status information for an MV
func (p *Provisioner) GetMVStatus(ctx context.Context, mvName string) (*MVStatus, error) {
	status := &MVStatus{Name: mvName}

	// Check if MV exists
	var count uint64
	row := p.client.QueryRow(ctx, `
		SELECT count() 
		FROM system.tables 
		WHERE database = currentDatabase() 
		  AND name = ?
	`, mvName)
	if err := row.Scan(&count); err != nil {
		return nil, fmt.Errorf("check MV exists: %w", err)
	}
	status.Exists = count > 0

	if !status.Exists {
		return status, nil
	}

	// Get refresh info
	var lastRefresh, nextRefresh time.Time
	row = p.client.QueryRow(ctx, `
		SELECT 
			coalesce(last_refresh_time, toDateTime(0)),
			coalesce(next_refresh_time, toDateTime(0))
		FROM system.view_refreshes 
		WHERE database = currentDatabase() 
		  AND view = ?
	`, mvName)
	if err := row.Scan(&lastRefresh, &nextRefresh); err == nil {
		status.LastRefresh = lastRefresh
		status.NextRefresh = nextRefresh
	}

	// Get row count from target table (approximate)
	targetTable := strings.TrimSuffix(mvName, "_mv")
	row = p.client.QueryRow(ctx, `
		SELECT count() 
		FROM ? 
	`, targetTable)
	if err := row.Scan(&status.RowCount); err != nil {
		// Table may not exist or have different name, ignore error
		status.RowCount = 0
	}

	return status, nil
}



