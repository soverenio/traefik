package confik

import (
	"errors"
	"fmt"
	"github.com/traefik/traefik/v2/pkg/config/dynamic"
	"gopkg.in/yaml.v3"
	"os"
	"path"
	"path/filepath"
	"strings"
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

	out, err := yaml.Marshal(conf)
	if err != nil {
		return err
	}

	err = writeFile(filename, out)
	if err != nil {
		return err
	}
	return nil
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
