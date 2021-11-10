package soveren

import (
	"github.com/traefik/traefik/v2/pkg/config/dynamic"
	"github.com/traefik/traefik/v2/pkg/provider"
	"github.com/traefik/traefik/v2/pkg/safe"
)

var _ provider.Provider = (*Provider)(nil)

type Provider struct {
	dynamic.Replicate
}

// Provide allows the soveren provider to provide configurations to traefik
// using the given configuration channel.
func (p *Provider) Provide(_ chan<- dynamic.Message, _ *safe.Pool) error {
	return nil
}

func (p *Provider) Init() error {
	return nil
}
