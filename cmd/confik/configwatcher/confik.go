package configwatcher

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/traefik/traefik/v2/pkg/provider/file"
	"github.com/traefik/traefik/v2/pkg/provider/soveren"

	"github.com/traefik/traefik/v2/pkg/config/dynamic"
	"github.com/traefik/traefik/v2/pkg/config/static"
	"github.com/traefik/traefik/v2/pkg/log"
	"github.com/traefik/traefik/v2/pkg/provider/aggregator"
	"github.com/traefik/traefik/v2/pkg/safe"

	"github.com/traefik/paerser/cli"
)

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

func runCmd(traefikConfiguration *static.Configuration) error {
	svr := setupDaemon(traefikConfiguration)

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

	watcher := NewConfigurationWatcher(
		routinesPool,
		providerAggregator,
		time.Duration(staticConfiguration.Providers.ProvidersThrottleDuration),
		[]string{},
		"",
	)

	watcher.AddListener(mergeWith(staticConfiguration.Providers.Soveren, staticConfiguration.Providers.File))

	return &Daemon{
		watcher:      watcher,
		signals:      make(chan os.Signal, 1),
		stopChan:     make(chan bool, 1),
		routinesPool: routinesPool,
	}
}
