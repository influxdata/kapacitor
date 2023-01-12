package server

import (
	"encoding/json"

	"github.com/influxdata/kapacitor/services/storage"
	"github.com/mailru/easyjson/jwriter"

	"github.com/mailru/easyjson/jlexer"
	"github.com/pkg/errors"

	"go.etcd.io/bbolt"
)

const (
	topicStatesNameSpaceV2 = "topic_states_store"
	alertNameSpace         = "alert_store"
	versionStoreNameSpace  = "version"
	topicStoreVersionKey   = "topic_store_version"
)

func (s *Server) MigrateTopicStore() error {
	version, err := s.AlertService.StorageService.Versions().Get(topicStoreVersionKey)
	if err != nil {
		return err
	}
	if version == "2" {
		return nil
	}
	return s.AlertService.StorageService.Store(topicStatesNameSpaceV2).Update(func(tx storage.Tx) error {
		v1Bucket := []byte(alertNameSpace)
		b := tx.Bucket(nil).Bucket(v1Bucket) // the read bucket
		if b == nil {
			return errors.New("come up with error")
		}

		// read the data from the v1 bucket and write the data to the v2 bucket
		lex := &jlexer.Lexer{}
		cursor := b.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			topicOutBucket := tx.Bucket(k)
			lex.Data = v
			processJSON(lex, func(eventID, eventVal []byte) error {
				return topicOutBucket.Put(string(eventID), eventVal)
			})
			if err := lex.Error(); err != nil {
				return err
			}
		}
		return nil
	})
}

func MigrateTopicStoreV2V1(db *bbolt.DB) (err error) {
	v2Bucket := []byte(topicStatesNameSpaceV2)
	v1Bucket := []byte(alertNameSpace)

	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(v2Bucket)
		if b == nil {
			return errors.New("come up with error")
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
}

//easyjson:json
type TopicStateV1 struct {
	Version string `json:"version"`
	Value   struct {
		Topic       string                     `json:"topic"`
		EventStates map[string]json.RawMessage `json:"event-states"`
	} `json:"value"`
}

func (t *TopicStateV1) ObjectID() string {
	return t.Value.Topic
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
