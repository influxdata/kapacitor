package alert

import (
	"fmt"

	"github.com/influxdata/kapacitor/services/storage"
	"github.com/pkg/errors"
)

const (
	TopicStoreVersionKey = "topic_store_version"
	TopicStoreVersion2   = "2"
)

func (s *Service) MigrateTopicStore() error {
	version, err := s.StorageService.Versions().Get(TopicStoreVersionKey)
	if err != nil && !errors.Is(err, storage.ErrNoKeyExists) {
		return err
	}
	if version == TopicStoreVersion2 {
		return nil
	}
	topicsDAO, err := NewTopicStateKV(s.StorageService.Store(AlertNameSpace))
	if err != nil {
		return fmt.Errorf("cannot create version 1 topic store: %w", err)
	}

	offset := 0
	const limit = 100

	topicKeys := make([]string, 0, limit)
	err = s.StorageService.Store(TopicStatesNameSpace).Update(func(txV2 storage.Tx) error {
		for {
			topicStates, err := topicsDAO.List("", offset, limit)
			if err != nil {
				return fmt.Errorf("cannot read version 1 topic store: %w", err)
			}
			for _, ts := range topicStates {
				topicKeys = append(topicKeys, ts.Topic)
				txBucket := txV2.Bucket([]byte(ts.Topic))
				for id, es := range ts.EventStates {
					data, err := es.MarshalJSON()
					if err != nil {
						return fmt.Errorf("error converting event %q in topic %q to JSON: %w", id, ts.Topic, err)
					}
					if err = txBucket.Put(id, data); err != nil {
						return fmt.Errorf("cannot store event %q in topic %q: %w", id, ts.Topic, err)
					}
				}
			}
			offset += limit
			// TODO(DSB): check what happens if exactly limit are present....  Does List() EOF or return empty?
			if len(topicStates) != limit {
				break
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err = topicsDAO.DeleteMultiple(topicKeys); err != nil {
		return err
	}
	if err = s.StorageService.Versions().Set(TopicStoreVersionKey, TopicStoreVersion2); err != nil {
		return fmt.Errorf("cannot set topic store version to %s: %w", TopicStoreVersion2, err)
	}
	return nil
}

func MigrateTopicStoreV2V1(storageService StorageService) (err error) {

	version, err := storageService.Versions().Get(TopicStoreVersionKey)
	if err != nil && !errors.Is(err, storage.ErrNoKeyExists) {
		return err
	}
	if errors.Is(err, storage.ErrNoKeyExists) || (version != TopicStoreVersion2) {
		// V1 has no version number
		return nil
	}

	topicsDAO, err := NewTopicStateKV(storageService.Store(AlertNameSpace))
	if err != nil {
		return fmt.Errorf("cannot create version 1 topic store: %w", err)
	}

	topicsStore := storageService.Store(TopicStatesNameSpace)

	topics := make([]TopicState, 0, 100)
	err = WalkTopicBuckets(topicsStore, func(tx storage.ReadOnlyTx, topic string) error {
		eventStates, err := LoadTopicBucket(tx, []byte(topic))
		if err != nil {
			return err
		}
		topics = append(topics, TopicState{Topic: topic, EventStates: eventStates})
		return nil
	})
	if err != nil {
		return err
	}

	for i, _ := range topics {
		if err = topicsDAO.Put(topics[i]); err != nil {
			return fmt.Errorf("cannot save topic %q: %w", topics[i].Topic, err)
		}
	}

	if err = DeleteV2TopicStore(topicsStore); err != nil {
		return err
	}
	return storageService.Versions().Set(TopicStoreVersionKey, "")
}

func DeleteV2TopicStore(topicsStore storage.Interface) error {
	return topicsStore.Update(func(txV2 storage.Tx) error {
		kv, err := txV2.List("")
		if err != nil {
			return fmt.Errorf("cannot retrieve version 2 topic list: %w", err)
		}

		for _, b := range kv {
			if b == nil {
				continue
			}
			if err = txV2.Delete(b.Key); err != nil {
				return err
			}
		}
		return nil
	})
}

func LoadTopicBucket(tx storage.ReadOnlyTx, topic []byte) (map[string]EventState, error) {
	q, err := tx.Bucket(topic).List("")
	if err != nil {
		return nil, err
	}
	EventStates := make(map[string]EventState, len(q))
	es := &EventState{} //create a buffer to hold the unmarshalled EventState
	for _, b := range q {
		err = es.UnmarshalJSON(b.Value)
		if err != nil {
			return nil, err
		}
		EventStates[b.Key] = *es
		es.Reset()
	}
	return EventStates, nil
}
