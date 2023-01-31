package alert

import (
	"fmt"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/pkg/errors"
)

const (
	TopicStoreVersionKey = "topic_store_version"
	TopicStoreVersion2   = "2"
)

// MigrateTopicStoreV1V2 - Convert a V1 to a V2 topic store.
// Also ensures that a topic store has a V2 version number set.
func (s *Service) MigrateTopicStoreV1V2() error {
	version, err := s.StorageService.Versions().Get(TopicStoreVersionKey)
	if err != nil && !errors.Is(err, storage.ErrNoKeyExists) {
		return fmt.Errorf("cannot determine topic store version: %w", err)
	}
	if version == TopicStoreVersion2 {
		s.diag.Info(fmt.Sprintf("Topic Store is already version %s. Cannot upgrade.", TopicStoreVersion2))
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
	s.diag.Info("Topic Store updated", keyvalue.T{Key: "version", Value: TopicStoreVersion2})
	return nil
}

func MigrateTopicStoreV2V1(storageService StorageService) error {

	version, err := storageService.Versions().Get(TopicStoreVersionKey)
	if err != nil && !errors.Is(err, storage.ErrNoKeyExists) {
		return fmt.Errorf("cannot determine topic store version: %w", err)
	}
	if errors.Is(err, storage.ErrNoKeyExists) || (version != TopicStoreVersion2) {
		// V1 has no version number
		storageService.Diagnostic().Info(fmt.Sprintf("Topic Store is not version %s. Cannot downgrade.", TopicStoreVersion2))
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
			return fmt.Errorf("cannot load topic %q: %w", topic, err)
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
	if err = storageService.Versions().Set(TopicStoreVersionKey, ""); err != nil {
		return fmt.Errorf("cannot set topic store version to %s after upgrade: %w", TopicStoreVersion2, err)
	}
	storageService.Diagnostic().Info("Topic Store upgraded", keyvalue.T{Key: "version", Value: TopicStoreVersion2})
	return nil
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
				return fmt.Errorf("cannot delete topic %q: %w", b.Key, err)

			}
		}
		return nil
	})
}

func LoadTopicBucket(tx storage.ReadOnlyTx, topic []byte) (map[string]EventState, error) {
	q, err := tx.Bucket(topic).List("")
	if err != nil {
		return nil, fmt.Errorf("cannot load topic %q: %w", topic, err)
	}
	EventStates := make(map[string]EventState, len(q))
	es := &EventState{} //create a buffer to hold the unmarshalled EventState
	for _, b := range q {
		err = es.UnmarshalJSON(b.Value)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal an event in topic %q: %w", topic, err)
		}
		EventStates[b.Key] = *es
		es.Reset()
	}
	return EventStates, nil
}
