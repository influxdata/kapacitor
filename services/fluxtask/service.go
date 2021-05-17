package fluxtask

import (
	"context"
	"time"

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
	"github.com/influxdata/kapacitor/task/fluxlocal"
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
			Bucket:      s.config.TaskRunBucket,
			Org:         s.config.TaskRunOrg,
			OrgID:       s.config.TaskRunOrgID,
			Measurement: s.config.TaskRunMeasurement,
		}

		var (
			taskService        taskmodel.TaskService      = s.kvService
			taskControlService backend.TaskControlService = s.kvService
		)

		if s.config.TaskRunInfluxDB != "none" {
			cli, err := s.InfluxDBService.NewNamedClient(s.config.TaskRunInfluxDB)
			if err != nil {
				return err
			}
			combinedTaskService := backend.NewAnalyticalStorage(
				s.logger.With(zap.String("service", "fluxtask-analytical-store")),
				taskService,
				taskControlService,
				cli,
				dataDestination,
			)
			taskService = combinedTaskService
			taskControlService = combinedTaskService
		}
		// TODO: register metrics returned here?
		executor, _ := executor.NewExecutor(
			s.logger.With(zap.String("service", "fluxtask-executor")),
			fluxlocal.NewFluxQueryer(s.config.Secrets, s.logger.With(zap.String("service", "flux-local-querier"))),
			taskService,
			taskControlService,
		)
		var err error
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
		taskService = middleware.New(taskService, taskCoord)
		if err := backend.TaskNotifyCoordinatorOfExisting(
			context.Background(),
			taskService,
			taskControlService,
			taskCoord,
			func(ctx context.Context, taskID platform.ID, runID platform.ID) error {
				_, err := executor.ResumeCurrentRun(ctx, taskID, runID)
				return err
			},
			coordLogger); err != nil {
			s.logger.Error("Failed to resume existing flux tasks", zap.Error(err))
		}
		if err := http.AddTaskServiceRoutes(s.HTTPDService.Handler, s.logger, taskService); err != nil {
			s.logger.Fatal("Could not add task service routes", zap.Error(err))
		}
	}
	return nil
}

func (s *Service) Close() error {
	if s.kvService != nil {
		if err := s.kvService.Close(); err != nil {
			return err
		}
		s.kvService = nil
	}
	if s.scheduler != nil {
		s.scheduler.Stop()
		s.scheduler = nil
	}
	if err := http.RemoveTaskServiceRoutes(s.HTTPDService.Handler); err != nil {
		return err
	}
	return nil
}
