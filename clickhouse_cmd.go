package main

import (
	"context"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"path/filepath"
	"time"

	"github.com/bufbuild/connect-go"
	"github.com/fsnotify/fsnotify"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	connectprometheus "github.com/polarsignals/connect-go-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/pyrra-dev/pyrra/clickhouse"
	"github.com/pyrra-dev/pyrra/proto/objectives/v1alpha1/objectivesv1alpha1connect"
	"github.com/pyrra-dev/pyrra/slo"
)

func cmdClickHouse(
	logger log.Logger,
	reg *prometheus.Registry,
	chConfig clickhouse.Config,
	configFiles string,
	genericRules bool,
) int {
	// Create metrics
	reconcilesTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pyrra_clickhouse_reconciles_total",
		Help: "The total amount of reconciles.",
	})
	reconcilesErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pyrra_clickhouse_reconciles_errors_total",
		Help: "The total amount of errors during reconciles.",
	})
	mvProvisionedTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pyrra_clickhouse_mv_provisioned_total",
		Help: "The total number of MVs provisioned.",
	})
	mvProvisionErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pyrra_clickhouse_mv_provision_errors_total",
		Help: "The total number of MV provisioning errors.",
	})

	reg.MustRegister(
		reconcilesTotal,
		reconcilesErrors,
		mvProvisionedTotal,
		mvProvisionErrors,
	)

	// Ensure database exists before connecting
	if err := clickhouse.EnsureDatabase(context.Background(), chConfig); err != nil {
		level.Error(logger).Log("msg", "failed to ensure database exists", "err", err)
		return 1
	}

	// Create ClickHouse client
	client, err := clickhouse.NewClient(chConfig)
	if err != nil {
		level.Error(logger).Log("msg", "failed to create ClickHouse client", "err", err)
		return 1
	}
	defer client.Close()

	// Run migrations
	migrator := clickhouse.NewMigrator(client, logger)
	if err := migrator.RunMigrations(context.Background()); err != nil {
		level.Error(logger).Log("msg", "failed to run ClickHouse migrations", "err", err)
		return 1
	}

	// Create provisioner
	provisioner := clickhouse.NewProvisioner(client, chConfig, logger)

	// Sync existing MVs from database
	if err := provisioner.SyncFromDatabase(context.Background()); err != nil {
		level.Warn(logger).Log("msg", "failed to sync MVs from database", "err", err)
	}

	// Extract UI files from embedded filesystem
	build, err := fs.Sub(ui, "ui/build")
	if err != nil {
		level.Error(logger).Log("msg", "failed to read UI build files", "err", err)
		return 1
	}

	tmpl, err := template.ParseFS(build, "index.html")
	if err != nil {
		level.Error(logger).Log("msg", "failed to parse HTML template", "err", err)
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	objectives := &Objectives{objectives: map[string]slo.Objective{}}
	files := make(chan string, 16)

	var gr run.Group
	{
		// Initial file loading
		gr.Add(func() error {
			filenames, err := filepath.Glob(configFiles)
			if err != nil {
				return fmt.Errorf("getting files names: %w", err)
			}
			for _, f := range filenames {
				files <- f
			}
			<-ctx.Done()
			return nil
		}, func(_ error) {
			cancel()
		})
	}
	{
		// File watcher
		dir := filepath.Dir(configFiles)
		level.Info(logger).Log("msg", "watching directory for changes", "directory", dir)

		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			level.Error(logger).Log("msg", "failed to create file watcher", "err", err)
			return 1
		}

		if err := watcher.Add(dir); err != nil {
			level.Error(logger).Log("msg", "failed to add directory to file watcher", "directory", dir, "err", err)
			return 1
		}

		gr.Add(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case event, ok := <-watcher.Events:
					if !ok {
						continue
					}
					if event.Op&fsnotify.Write == fsnotify.Write {
						files <- event.Name
					}
				case err := <-watcher.Errors:
					level.Warn(logger).Log("msg", "encountered file watcher error", "err", err)
				}
			}
		}, func(_ error) {
			_ = watcher.Close()
			cancel()
		})
	}
	{
		// File processor
		gr.Add(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case f := <-files:
					if filepath.Ext(f) != ".yaml" && filepath.Ext(f) != ".yml" {
						level.Warn(logger).Log("msg", "ignoring non YAML file", "file", f)
						continue
					}

					level.Debug(logger).Log("msg", "processing", "file", f)
					reconcilesTotal.Inc()

					_, objective, err := objectiveFromFile(f)
					if err != nil {
						reconcilesErrors.Inc()
						level.Error(logger).Log("msg", "failed to get objective from file", "file", f, "err", err)
						continue
					}

					objectives.Set(objective)

					// Provision MVs for this objective
					if err := provisioner.ProvisionObjective(ctx, objective); err != nil {
						mvProvisionErrors.Inc()
						level.Error(logger).Log("msg", "failed to provision MVs", "objective", objective.Name(), "err", err)
					} else {
						mvProvisionedTotal.Inc()
						level.Info(logger).Log("msg", "provisioned MVs", "objective", objective.Name())
					}
				}
			}
		}, func(_ error) {
			cancel()
		})
	}
	{
		// Periodic MV reconciliation
		gr.Add(func() error {
			ticker := time.NewTicker(5 * time.Minute)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
					objs := objectives.Match(nil)
					if err := provisioner.ReconcileAll(ctx, objs); err != nil {
						level.Error(logger).Log("msg", "failed to reconcile MVs", "err", err)
					}
				}
			}
		}, func(_ error) {
			cancel()
		})
	}
	{
		// HTTP server serving both the UI and API
		prometheusInterceptor := connectprometheus.NewInterceptor(reg)

		// Create the full ObjectiveService that queries ClickHouse directly via SQL
		objService := &clickhouseObjectiveService{
			logger:     log.WithPrefix(logger, "service", "objective"),
			client:     client,
			objectives: objectives,
		}

		r := chi.NewRouter()
		r.Use(cors.Handler(cors.Options{
			AllowedHeaders: []string{
				"Content-Type",
				"Connect-Protocol-Version",
			},
		}))

		// Register the full ObjectiveServiceHandler (not just the backend)
		objectivePath, objectiveHandler := objectivesv1alpha1connect.NewObjectiveServiceHandler(
			objService,
			connect.WithInterceptors(prometheusInterceptor),
		)
		r.Mount(objectivePath, objectiveHandler)

		// Metrics and health endpoints
		r.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		r.Get("/ready", func(w http.ResponseWriter, r *http.Request) {
			if err := client.Ready(r.Context()); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprintf(w, "ClickHouse not ready: %v", err)
				return
			}
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "OK")
		})

		// UI template data
		templateData := struct {
			ExternalURL                 string
			ExternalGrafanaDatasourceID string
			ExternalGrafanaOrgID        string
			PathPrefix                  string
			APIBasepath                 string
		}{
			PathPrefix:  "/",
			APIBasepath: "/",
		}

		// Serve the UI
		r.Get("/objectives", func(w http.ResponseWriter, _ *http.Request) {
			if err := tmpl.Execute(w, templateData); err != nil {
				level.Warn(logger).Log("msg", "failed to populate HTML template", "err", err)
			}
		})
		r.Get("/*", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" {
				if err := tmpl.Execute(w, templateData); err != nil {
					level.Warn(logger).Log("msg", "failed to populate HTML template", "err", err)
				}
				return
			}
			http.FileServer(http.FS(build)).ServeHTTP(w, r)
		}))

		server := http.Server{
			Addr:    ":9099",
			Handler: h2c.NewHandler(r, &http2.Server{}),
		}

		gr.Add(func() error {
			level.Info(logger).Log("msg", "starting up ClickHouse backend with UI", "address", server.Addr)
			return server.ListenAndServe()
		}, func(_ error) {
			_ = server.Shutdown(context.Background())
		})
	}

	if err := gr.Run(); err != nil {
		level.Error(logger).Log("msg", "failed to run", "err", err)
		return 2
	}
	return 0
}
