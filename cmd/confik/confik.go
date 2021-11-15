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

	ptypes "github.com/traefik/paerser/types"

	tcli "github.com/traefik/traefik/v2/pkg/cli"
	"github.com/traefik/traefik/v2/pkg/metrics"
	"github.com/traefik/traefik/v2/pkg/types"

	"github.com/sirupsen/logrus"

	"github.com/traefik/traefik/v2/pkg/config/dynamic"
	"github.com/traefik/traefik/v2/pkg/config/static"
	"github.com/traefik/traefik/v2/pkg/log"
	"github.com/traefik/traefik/v2/pkg/provider/aggregator"
	"github.com/traefik/traefik/v2/pkg/safe"

	"github.com/traefik/paerser/cli"
)

func main() {
	// confik settings
	cfg := NewStaticConfiguration()

	loaders := []cli.ResourceLoader{&tcli.FileLoader{}, &tcli.FlagLoader{}, &tcli.EnvLoader{}}

	cmdConfik := NewCmd(cfg, loaders)

	err := cli.Execute(cmdConfik)
	if err != nil {
		stdlog.Println(err)
		logrus.Exit(1)
	}

	logrus.Exit(0)
}

func configureLogging(staticConfiguration *StaticConfiguration) {
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

type StaticConfiguration struct {
	Providers   *static.Providers
	EntryPoints static.EntryPoints `description:"Entry points definition." json:"entryPoints,omitempty" toml:"entryPoints,omitempty" yaml:"entryPoints,omitempty" export:"true"`
	Metrics     *types.Metrics     `description:"Enable a metrics exporter." json:"metrics,omitempty" toml:"metrics,omitempty" yaml:"metrics,omitempty" export:"true"`
	Log         *types.TraefikLog  `description:"Confik log settings." json:"log,omitempty" toml:"log,omitempty" yaml:"log,omitempty" label:"allowEmpty" file:"allowEmpty" export:"true"`

	OutputDirectory string `description:"Save dynamic configuration to directory." json:"output_directory,omitempty" toml:"output_directory,omitempty" yaml:"output_directory,omitempty" export:"true"`
	MiddlewareName  string `description:"Name of injected middleware." json:"middleware_name,omitempty" toml:"middleware_name,omitempty" yaml:"middleware_name,omitempty" export:"true"`
}

func NewStaticConfiguration() *StaticConfiguration {
	return &StaticConfiguration{
		Providers: &static.Providers{
			ProvidersThrottleDuration: ptypes.Duration(2 * time.Second),
		},
		EntryPoints: make(static.EntryPoints),
	}
}

func export(staticConf *StaticConfiguration) func(dynamic.Message) {
	return func(msg dynamic.Message) {
		if staticConf == nil {
			log.WithoutContext().Info("StaticConfiguration is nil, exporting skipped")
			return
		}
		if len(staticConf.OutputDirectory) == 0 || len(staticConf.MiddlewareName) == 0 {
			log.WithoutContext().Info("OutputDirectory or MiddlewareName is an empty, exporting skipped")
			return
		}

		conf := msg.Configuration
		for _, router := range conf.HTTP.Routers {
			skipRouter := false
			for _, m := range router.Middlewares {
				if m == staticConf.MiddlewareName {
					skipRouter = true
					break
				}
			}

			if skipRouter {
				continue
			}
			router.Middlewares = append(router.Middlewares, staticConf.MiddlewareName)
		}
		err := saveConfiguration(staticConf.OutputDirectory, conf)
		if err != nil {
			log.WithoutContext().Error(err.Error())
		}
	}
}

func NewCmd(traefikConfiguration *StaticConfiguration, loaders []cli.ResourceLoader) *cli.Command {
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

func runCmd(staticConfiguration *StaticConfiguration) error {
	configureLogging(staticConfiguration)

	svr := setupDaemon(staticConfiguration)

	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	svr.Start(ctx)
	defer svr.Close()

	svr.Wait()
	log.WithoutContext().Info("Shutting down")
	return nil
}

func setupDaemon(staticConfiguration *StaticConfiguration) *Daemon {
	providerAggregator := aggregator.NewProviderAggregator(*staticConfiguration.Providers)

	ctx := context.Background()
	routinesPool := safe.NewPool(ctx)

	registerMetricClients(staticConfiguration.Metrics)

	metricServer := NewMetricServer(staticConfiguration, routinesPool)

	watcher := NewConfigurationWatcher(
		routinesPool,
		providerAggregator,
		time.Duration(staticConfiguration.Providers.ProvidersThrottleDuration),
		[]string{},
		"",
	)

	watcher.AddListener(export(staticConfiguration))

	return &Daemon{
		watcher:      watcher,
		signals:      make(chan os.Signal, 1),
		stopChan:     make(chan bool, 1),
		routinesPool: routinesPool,
		metricSrv:    metricServer,
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

	return registries
}
