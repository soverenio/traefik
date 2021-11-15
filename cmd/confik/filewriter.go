package main

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/traefik/traefik/v2/pkg/config/dynamic"
	"gopkg.in/yaml.v3"
)

const (
	defaultFileName = "traefikcfg.yaml"
	perm            = os.FileMode(0777)
)

func saveConfiguration(filename, directory string, conf *dynamic.Configuration) error {
	if conf == nil {
		return nil
	}
	filename, err := genFileName(filename, directory)
	if err != nil {
		return err
	}

	// To bypass errors because of empty config sections
	removeEmptySections(conf)

	buf, err := yaml.Marshal(conf)
	if err != nil {
		return err
	}

	err = writeFile(filename, buf)
	if err != nil {
		return err
	}
	return nil
}

func removeEmptySections(conf *dynamic.Configuration) {
	if conf.HTTP != nil {
		if len(conf.HTTP.Routers) == 0 &&
			len(conf.HTTP.Services) == 0 &&
			len(conf.HTTP.Middlewares) == 0 &&
			len(conf.HTTP.Models) == 0 &&
			len(conf.HTTP.ServersTransports) == 0 {
			conf.HTTP = nil
		}
	}

	if conf.TCP != nil {
		if len(conf.TCP.Routers) == 0 &&
			len(conf.TCP.Services) == 0 {
			conf.TCP = nil
		}
	}

	if conf.UDP != nil {
		if len(conf.UDP.Routers) == 0 &&
			len(conf.UDP.Services) == 0 {
			conf.UDP = nil
		}
	}

	if conf.TLS != nil {
		if len(conf.TLS.Certificates) == 0 &&
			len(conf.TLS.Options) == 0 &&
			len(conf.TLS.Stores) == 0 {
			conf.TLS = nil
		}
	}
}

func genFileName(filename, directory string) (string, error) {
	if len(filename) == 0 {
		if len(directory) == 0 {
			return "", errors.New("error using file configuration provider, neither filename or directory defined")
		}
		return path.Join(directory, defaultFileName), nil
	}
	return verifyExt(filename), nil
}

func verifyExt(filename string) string {
	if ext := filepath.Ext(filename); ext != "yaml" && ext != "yml" {
		return strings.TrimSuffix(filename, filepath.Ext(filename)) + ".yaml"
	}
	return filename
}

func writeFile(filename string, conf []byte) error {
	if len(conf) == 0 {
		return errors.New("empty config")
	}
	if len(filename) == 0 {
		return fmt.Errorf("invalid filename: %s", filename)
	}
	err := os.WriteFile(filename, conf, perm)
	if err != nil {
		return err
	}
	return nil
}
