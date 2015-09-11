package streamer

import (
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/kapacitor/models"
)

type Service struct {
	StreamCollector interface {
		CollectPoint(*models.Point) error
	}
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) WritePoints(pts *cluster.WritePointsRequest) (err error) {
	for _, mp := range pts.Points {
		p := models.NewPoint(
			mp.Name(),
			models.NilGroup,
			mp.Tags(),
			mp.Fields(),
			mp.Time(),
		)
		err = s.StreamCollector.CollectPoint(p)
		if err != nil {
			return
		}
	}
	return
}
