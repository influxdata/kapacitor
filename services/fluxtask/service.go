package fluxtask

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/influxdb"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/task"
	"github.com/influxdata/kapacitor/task/backend"
	"github.com/influxdata/kapacitor/task/backend/coordinator"
	"github.com/influxdata/kapacitor/task/backend/executor"
	"github.com/influxdata/kapacitor/task/backend/middleware"
	"github.com/influxdata/kapacitor/task/backend/scheduler"
	"github.com/influxdata/kapacitor/task/http"
	"github.com/influxdata/kapacitor/task/kv"
	"github.com/influxdata/kapacitor/task/taskmodel"
	"go.uber.org/zap"
)

type Service struct {
	config          task.Config
	logger          *zap.Logger
	kvService       *kv.Service
	HTTPDService    *httpd.Service
	InfluxDBService *influxdb.Service
	StorageService  *storage.Service
	scheduler       *scheduler.TreeScheduler
}

func New(config task.Config, logger *zap.Logger) *Service {
	return &Service{
		config: config,
		logger: logger,
	}
}

func (s *Service) Open() error {
	if s.config.Enabled {
		// create the task stack
		s.kvService = kv.New(s.StorageService)
		s.kvService.Open()
		dataDestination := backend.DataDestination{
			Bucket: s.config.TaskRunBucket,
			Org:    s.config.TaskRunOrg,
			OrgID:  s.config.TaskRunOrgID,
		}
		cli, err := s.InfluxDBService.NewNamedClient(s.config.TaskRunInfluxDB)
		if err != nil {
			return err
		}
		combinedTaskService := backend.NewAnalyticalStorage(
			s.logger.With(zap.String("service", "fluxtask-analytical-store")),
			s.kvService,
			s.kvService,
			cli,
			dataDestination,
		)
		// TODO: register metrics returned here?
		executor, _ := executor.NewExecutor(
			s.logger.With(zap.String("service", "fluxtask-executor")),
			&DummyQueryService{logger: s.logger.With(zap.String("service", "fluxtask-dummy-query-executor"))},
			combinedTaskService,
			combinedTaskService,
		)
		schLogger := s.logger.With(zap.String("service", "fluxtask-scheduler"))
		//TODO: register metrics returned here?
		s.scheduler, _, err = scheduler.NewScheduler(
			executor,
			backend.NewSchedulableTaskService(s.kvService),
			scheduler.WithOnErrorFn(func(ctx context.Context, taskID scheduler.ID, scheduledAt time.Time, err error) {
				schLogger.Info(
					"error in scheduler run",
					zap.String("taskID", platform.ID(taskID).String()),
					zap.Time("scheduledAt", scheduledAt),
					zap.Error(err))
			}),
		)
		if err != nil {
			s.logger.Fatal("could not start task scheduler", zap.Error(err))
		}
		coordLogger := s.logger.With(zap.String("service", "fluxtask-coordinator"))
		taskCoord := coordinator.NewCoordinator(
			coordLogger,
			s.scheduler,
			executor)
		fluxTaskService := middleware.New(combinedTaskService, taskCoord)
		if err := backend.TaskNotifyCoordinatorOfExisting(
			context.Background(),
			fluxTaskService,
			combinedTaskService,
			taskCoord,
			func(ctx context.Context, taskID platform.ID, runID platform.ID) error {
				_, err := executor.ResumeCurrentRun(ctx, taskID, runID)
				return err
			},
			coordLogger); err != nil {
			s.logger.Error("Failed to resume existing flux tasks", zap.Error(err))
		}
		if err := http.AddTaskServiceRoutes(s.HTTPDService.Handler, s.logger, fluxTaskService); err != nil {
			s.logger.Fatal("Could not add task service routes", zap.Error(err))
		}
	}
	return nil
}

func (s *Service) Close() error {
	if err := s.kvService.Close(); err != nil {
		return err
	}
	s.scheduler.Stop()
	return http.RemoveTaskServiceRoutes(s.HTTPDService.Handler)
}

// TODO: remove
type NoResultIterator struct{}

func (n NoResultIterator) More() bool {
	return false
}

func (n NoResultIterator) Next() flux.Result {
	panic("should not be called since More is always false")
}

func (n NoResultIterator) Release() {
}

func (n NoResultIterator) Err() error {
	return nil
}

func (n NoResultIterator) Statistics() flux.Statistics {
	return flux.Statistics{}
}

var _ flux.ResultIterator = &NoResultIterator{}

type DummyQueryService struct {
	logger *zap.Logger
}

func (d *DummyQueryService) Query(ctx context.Context, compiler flux.Compiler) (flux.ResultIterator, error) {
	d.logger.Info(fmt.Sprintf("DummyQueryService called with compiler of type %T", compiler))
	time.Sleep(30 * time.Second)
	d.logger.Info("DummyQueryService is done sleeping")
	return new(NoResultIterator), nil
}

var _ taskmodel.QueryService = &DummyQueryService{}
