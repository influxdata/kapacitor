package fluxlocal

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies"
	"github.com/influxdata/flux/dependencies/filesystem"
	"github.com/influxdata/flux/dependencies/http"
	"github.com/influxdata/flux/dependencies/secret"
	"github.com/influxdata/flux/dependencies/url"
	"github.com/influxdata/flux/dependency"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/influxdb/v2/kit/errors"
	"go.uber.org/zap"
)

var _ secret.Service = &fluxQueryer{}

type fluxQueryer struct {
	secrets             map[string]string
	logger              *zap.Logger
	defaultInfluxDBHost string
}

func (f *fluxQueryer) LoadSecret(ctx context.Context, k string) (string, error) {
	if val, ok := f.secrets[k]; ok {
		return val, nil
	}
	return "", fmt.Errorf("secret named %s not found", k)
}

func NewFluxQueryer(secrets map[string]string, defaultInfluxDBHost string, logger *zap.Logger) *fluxQueryer {
	return &fluxQueryer{
		secrets:             secrets,
		logger:              logger,
		defaultInfluxDBHost: defaultInfluxDBHost,
	}
}

func (f *fluxQueryer) injectDependencies(ctx context.Context) (context.Context, *dependency.Span) {
	validator := &url.PassValidator{}
	deps := dependencies.NewDefaultDependencies(f.defaultInfluxDBHost)

	deps.Deps = flux.Deps{
		Deps: flux.WrappedDeps{
			HTTPClient:        http.NewDefaultClient(validator),
			FilesystemService: filesystem.SystemFS,
			SecretService:     f,
			URLValidator:      validator,
		},
	}

	// inject the dependencies to the context.
	return dependency.Inject(ctx, deps)
}

func (f *fluxQueryer) Query(ctx context.Context, compiler flux.Compiler) (flux.ResultIterator, error) {
	f.logger.Info("executed flux query")
	var span *dependency.Span
	ctx, span = f.injectDependencies(context.Background())
	defer span.Finish()
	program, err := compiler.Compile(ctx, runtime.Default)
	query, err := program.Start(ctx, memory.DefaultAllocator)
	if err != nil {
		return nil, errors.Wrap(err, "error while executing flux program")
	}
	return &spannedResultIterator{
		ResultIterator: flux.NewResultIteratorFromQuery(query),
		span:           span,
	}, nil
}

type spannedResultIterator struct {
	flux.ResultIterator
	span *dependency.Span
}

func (s *spannedResultIterator) Release() {
	s.ResultIterator.Release()
	s.span.Finish()
}
