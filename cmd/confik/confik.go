package main

import (
	"context"
	stdlog "log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/traefik/traefik/v2/cmd"
	tcli "github.com/traefik/traefik/v2/pkg/cli"
	"github.com/traefik/traefik/v2/pkg/metrics"
	"github.com/traefik/traefik/v2/pkg/types"

	"github.com/traefik/traefik/v2/pkg/provider/file"
	"github.com/traefik/traefik/v2/pkg/provider/soveren"

	"github.com/traefik/traefik/v2/pkg/config/dynamic"
	"github.com/traefik/traefik/v2/pkg/config/static"
	"github.com/traefik/traefik/v2/pkg/log"
	"github.com/traefik/traefik/v2/pkg/provider/aggregator"
	"github.com/traefik/traefik/v2/pkg/safe"

	"github.com/traefik/paerser/cli"
)

func main() {
	// traefik config inits
	tConfig := cmd.NewTraefikConfiguration()

	loaders := []cli.ResourceLoader{&tcli.FileLoader{}, &tcli.FlagLoader{}, &tcli.EnvLoader{}}

	cmdConfik := NewCmd(&tConfig.Configuration, loaders)

	err := cli.Execute(cmdConfik)
	if err != nil {
		stdlog.Println(err)
		logrus.Exit(1)
	}

	logrus.Exit(0)
}

func configureLogging(staticConfiguration *static.Configuration) {
	// configure default log flags
	stdlog.SetFlags(stdlog.Lshortfile | stdlog.LstdFlags)

	// configure log level
	// an explicitly defined log level always has precedence. if none is
	// given and debug mode is disabled, the default is ERROR, and DEBUG
	// otherwise.
	levelStr := "error"
	if staticConfiguration.Log != nil && staticConfiguration.Log.Level != "" {
		levelStr = strings.ToLower(staticConfiguration.Log.Level)
	}

	level, err := logrus.ParseLevel(levelStr)
	if err != nil {
		log.WithoutContext().Errorf("Error getting level: %v", err)
	}
	log.SetLevel(level)

	var logFile string
	if staticConfiguration.Log != nil && len(staticConfiguration.Log.FilePath) > 0 {
		logFile = staticConfiguration.Log.FilePath
	}

	// configure log format
	var formatter logrus.Formatter
	if staticConfiguration.Log != nil && staticConfiguration.Log.Format == "json" {
		formatter = &logrus.JSONFormatter{}
	} else {
		disableColors := len(logFile) > 0
		formatter = &logrus.TextFormatter{DisableColors: disableColors, FullTimestamp: true, DisableSorting: true}
	}
	log.SetFormatter(formatter)

	if len(logFile) > 0 {
		dir := filepath.Dir(logFile)

		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.WithoutContext().Errorf("Failed to create log path %s: %s", dir, err)
		}

		err = log.OpenFile(logFile)
		logrus.RegisterExitHandler(func() {
			if err := log.CloseFile(); err != nil {
				log.WithoutContext().Errorf("Error while closing log: %v", err)
			}
		})
		if err != nil {
			log.WithoutContext().Errorf("Error while opening log file %s: %v", logFile, err)
		}
	}
}

func mergeWith(svrnProvider *soveren.Provider, fileProvider *file.Provider) func(dynamic.Message) {
	return func(msg dynamic.Message) {
		if svrnProvider == nil || fileProvider == nil {
			log.WithoutContext().Error("error using soveren or file configuration provider, one of the configs is not enough")
			return
		}

		if msg.ProviderName == "file" {
			log.WithoutContext().Info("Skipping configuration updates from file provider")
			return
		}

		conf := msg.Configuration
		conf.HTTP.Middlewares["soveren"] = &dynamic.Middleware{
			Replicate: &svrnProvider.Replicate,
		}
		for _, router := range conf.HTTP.Routers {
			router.Middlewares = append(router.Middlewares, "soveren")
		}
		err := saveConfiguration(fileProvider.Filename, fileProvider.Directory, conf)
		if err != nil {
			log.WithoutContext().Error(err.Error())
		}
	}
}

func NewCmd(traefikConfiguration *static.Configuration, loaders []cli.ResourceLoader) *cli.Command {
	return &cli.Command{
		Name:          "confik",
		Description:   `Run daemon that listens to some of the configuration providers, injects auxiliary properties then projects combined version to config destination`,
		Configuration: traefikConfiguration,
		Run: func(_ []string) error {
			return runCmd(traefikConfiguration)
		},
		Resources: loaders,
	}
}

func runCmd(staticConfiguration *static.Configuration) error {
	configureLogging(staticConfiguration)

	svr := setupDaemon(staticConfiguration)

	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	svr.Start(ctx)
	defer svr.Close()

	svr.Wait()
	log.WithoutContext().Info("Shutting down")
	return nil
}

func setupDaemon(staticConfiguration *static.Configuration) *Daemon {
	providerAggregator := aggregator.NewProviderAggregator(*staticConfiguration.Providers)

	ctx := context.Background()
	routinesPool := safe.NewPool(ctx)

	metricRegistries := registerMetricClients(staticConfiguration.Metrics)
	metricsRegistry := metrics.NewMultiRegistry(metricRegistries)

	watcher := NewConfigurationWatcher(
		routinesPool,
		providerAggregator,
		time.Duration(staticConfiguration.Providers.ProvidersThrottleDuration),
		[]string{},
		"",
	)

	watcher.AddListener(mergeWith(staticConfiguration.Providers.Soveren, staticConfiguration.Providers.File))

	// Metrics
	watcher.AddListener(func(_ dynamic.Message) {
		metricsRegistry.ConfigReloadsCounter().Add(1)
		metricsRegistry.LastConfigReloadSuccessGauge().Set(float64(time.Now().Unix()))
	})

	return &Daemon{
		watcher:      watcher,
		signals:      make(chan os.Signal, 1),
		stopChan:     make(chan bool, 1),
		routinesPool: routinesPool,
	}
}

func registerMetricClients(metricsConfig *types.Metrics) []metrics.Registry {
	if metricsConfig == nil {
		return nil
	}

	var registries []metrics.Registry

	if metricsConfig.Prometheus != nil {
		ctx := log.With(context.Background(), log.Str(log.MetricsProviderName, "prometheus"))
		prometheusRegister := metrics.RegisterPrometheus(ctx, metricsConfig.Prometheus)
		if prometheusRegister != nil {
			registries = append(registries, prometheusRegister)
			log.FromContext(ctx).Debug("Configured Prometheus metrics")
		}
	}

	if metricsConfig.Datadog != nil {
		ctx := log.With(context.Background(), log.Str(log.MetricsProviderName, "datadog"))
		registries = append(registries, metrics.RegisterDatadog(ctx, metricsConfig.Datadog))
		log.FromContext(ctx).Debugf("Configured Datadog metrics: pushing to %s once every %s",
			metricsConfig.Datadog.Address, metricsConfig.Datadog.PushInterval)
	}

	if metricsConfig.StatsD != nil {
		ctx := log.With(context.Background(), log.Str(log.MetricsProviderName, "statsd"))
		registries = append(registries, metrics.RegisterStatsd(ctx, metricsConfig.StatsD))
		log.FromContext(ctx).Debugf("Configured StatsD metrics: pushing to %s once every %s",
			metricsConfig.StatsD.Address, metricsConfig.StatsD.PushInterval)
	}

	if metricsConfig.InfluxDB != nil {
		ctx := log.With(context.Background(), log.Str(log.MetricsProviderName, "influxdb"))
		registries = append(registries, metrics.RegisterInfluxDB(ctx, metricsConfig.InfluxDB))
		log.FromContext(ctx).Debugf("Configured InfluxDB metrics: pushing to %s once every %s",
			metricsConfig.InfluxDB.Address, metricsConfig.InfluxDB.PushInterval)
	}

	return registries
}
