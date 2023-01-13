package alert

import (
	"fmt"

	"github.com/influxdata/kapacitor/services/storage"
	"github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

const (
	topicStoreVersionKey = "topic_store_version"
	topicStoreVersion2   = "2"
)

func (s *Service) MigrateTopicStore() error {
	version, err := s.StorageService.Versions().Get(topicStoreVersionKey)
	if err != nil && !errors.Is(err, storage.ErrNoKeyExists) {
		return err
	}
	if version == topicStoreVersion2 {
		return nil
	}
	topicsDAO, err := newTopicStateKV(s.StorageService.Store(alertNameSpace))
	if err != nil {
		return fmt.Errorf("cannot create version 1 topic store: %w", err)
	}

	offset := 0
	const limit = 100

	topicKeys := make([]string, 0, limit)
	err = s.StorageService.Store(topicStatesNameSpace).Update(func(txV2 storage.Tx) error {
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
	if err = s.StorageService.Versions().Set(topicStoreVersionKey, topicStoreVersion2); err != nil {
		return fmt.Errorf("cannot set topic store version to %s: %w", topicStoreVersion2, err)
	}
	return nil
}

func (s *Service) MigrateTopicStoreV2V1(db *bbolt.DB) (err error) {
	v2Bucket := []byte(topicStatesNameSpace)
	v1Bucket := []byte(alertNameSpace)

	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(v2Bucket)
		if b == nil {
			return fmt.Errorf("version 2 topic store not found: %q", topicStatesNameSpace)
		}

		bOut, err := tx.CreateBucketIfNotExists(v1Bucket)
		if bOut != nil {
			return err
		}
		inCursor := b.Cursor()
		for topic, v := inCursor.First(); topic != nil; topic, v = inCursor.Next() {
			if v != nil {
				return errors.New("not a bucket")
			}
			w := &jwriter.Writer{}
			w.RawByte('{')
			w.String("version")
			w.RawByte(':')
			w.Int64Str(1)
			w.RawByte(',')
			w.String("value")
			w.RawByte(':')
			w.RawByte('{')
			w.String("topic")
			w.RawByte(':')
			w.String(string(topic))
			w.RawByte(',')
			w.String("event-states")
			w.RawByte(':')
			w.RawByte('{')

			eventBucket := b.Bucket(topic)
			if eventBucket == nil {
				w.RawByte('}')
				continue
			}
			eventCursor := eventBucket.Cursor()
			if eventCursor == nil {
				w.RawByte('}')
				continue
			}
			i := 0
			for eventK, v := eventCursor.First(); eventK != nil; eventK, v = eventCursor.Next() {
				if v == nil {
					continue
				}
				if i != 0 {
					w.RawByte(',')
				}
				w.String(string(eventK))
				w.RawByte(':')
				w.Raw(v, nil)
				i++
			}
			w.RawByte('}')
			w.RawByte('}')
			w.RawByte('}')

		}

		return nil
	})

	// TODO(DSB): change version, delete V2 buckets.
}

func processJSON(in *jlexer.Lexer, out func(k []byte, v []byte) error) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeFieldName(false)
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "value":
			in.Delim('{')
			for !in.IsDelim('}') {
				key := in.UnsafeFieldName(false)
				in.WantColon()
				if in.IsNull() {
					in.Skip()
					in.WantComma()
					continue
				}
				switch key {
				case "event-states":
					if in.IsNull() {
						in.Skip()
					} else {
						in.Delim('{')
						for !in.IsDelim('}') {
							key := in.UnsafeFieldName(false)
							in.WantColon()
							if err := out([]byte(key), in.Raw()); err != nil {
								in.AddError(err)
							}
						}
						in.WantComma()
					}
					in.Delim('}')

				default:
					in.SkipRecursive()
				}
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
