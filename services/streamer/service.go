package streamer

import (
	"log"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/kapacitor/models"
)

type Service struct {
	StreamCollector interface {
		CollectPoint(models.Point) error
	}
	logger *log.Logger
}

func NewService(l *log.Logger) *Service {
	return &Service{
		logger: l,
	}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) WritePoints(pts *cluster.WritePointsRequest) (err error) {
	for _, mp := range pts.Points {
		p := models.Point{
			Database:        pts.Database,
			RetentionPolicy: pts.RetentionPolicy,
			Name:            mp.Name(),
			Group:           models.NilGroup,
			Tags:            models.Tags(mp.Tags()),
			Fields:          models.Fields(mp.Fields()),
			Time:            mp.Time(),
		}
		err = s.StreamCollector.CollectPoint(p)
		if err != nil {
			s.logger.Println("E!", err)
			return
		}
	}
	return
}
