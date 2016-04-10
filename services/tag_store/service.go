package tag_store

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"regexp"

	"github.com/boltdb/bolt"
	"github.com/influxdata/kapacitor/services/httpd"
)

const tagDB = "tag.db"

var (
	tagsBucket       = []byte("tags")
	recordingsBucket = []byte("recordings")
)

type Tag struct {
	Tag string `json:"tag"`
	ID  string `json:"id"`
}

type Service struct {
	dbpath       string
	db           *bolt.DB
	routes       []httpd.Route
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
	logger *log.Logger
}

func NewService(conf Config, l *log.Logger) *Service {
	return &Service{
		dbpath: path.Join(conf.Dir, tagDB),
		logger: l,
	}
}

func (ts *Service) Open() (err error) {
	defer func() {
		if err != nil && ts.db != nil {
			ts.db.Close()
			ts.db = nil
		}
	}()

	err = os.MkdirAll(path.Dir(ts.dbpath), 0755)
	if err != nil {
		return err
	}

	// Open db
	db, err := bolt.Open(ts.dbpath, 0600, nil)
	if err != nil {
		return err
	}
	ts.db = db

	if err := ts.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(recordingsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(tagsBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// Define API routes
	ts.routes = []httpd.Route{
		{
			Name:        "tag-list",
			Method:      "GET",
			Pattern:     "/tags",
			HandlerFunc: ts.handleTags,
		},
		{
			Name:        "tag-save",
			Method:      "POST",
			Pattern:     "/tag",
			HandlerFunc: ts.handleCreate,
		},
		{
			Name:        "tag-delete",
			Method:      "DELETE",
			Pattern:     "/tag",
			HandlerFunc: ts.handleDelete,
		},
		{
			// Satisfy CORS checks.
			Name:        "tag-options",
			Method:      "OPTIONS",
			Pattern:     "/tag",
			HandlerFunc: httpd.ServeOptions,
		},
	}
	err = ts.HTTPDService.AddRoutes(ts.routes)
	if err != nil {
		return err
	}

	return nil
}

func (ts *Service) Close() error {
	ts.HTTPDService.DelRoutes(ts.routes)
	if ts.db != nil {
		return ts.db.Close()
	}
	return nil
}

func (ts *Service) handleDelete(w http.ResponseWriter, r *http.Request) {
	tag := r.URL.Query().Get("tag")

	err := ts.DeleteTag(tag)
	if err != nil {
		ts.logger.Printf("E! delete tags '%s' failed: %v", tag, err)
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (ts *Service) handleTags(w http.ResponseWriter, r *http.Request) {
	tags, err := ts.ListTags()
	if err != nil {
		ts.logger.Printf("E! tags failed: %v", err)
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	type response struct {
		Tags []Tag `json:"tags"`
	}

	w.Write(httpd.MarshalJSON(response{tags}, true))
}

func (ts *Service) handleCreate(w http.ResponseWriter, r *http.Request) {
	tag := r.URL.Query().Get("tag")
	id := r.URL.Query().Get("id")

	err := ts.CreateOrUpdateTag(tag, id)
	if err != nil {
		ts.logger.Printf("E! create tag '%s' failed: %v", tag, err)
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// 6af4b71c-0fc4-483d-9518-f52f7ba14403

var uuid_pattern = regexp.MustCompile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
var tag_chars = regexp.MustCompile("^[A-Za-z0-9_+.-]+$")

// ErrNotFound is returned if a matching key is not found
var ErrNotFound = errors.New("not found")

func read(b *bolt.Bucket, k string, o interface{}) error {
	if bytes := b.Get([]byte(k)); bytes == nil {
		return ErrNotFound
	} else {
		if err := json.Unmarshal(bytes, o); err != nil {
			return err
		}
	}
	return nil
}

func write(b *bolt.Bucket, k string, o interface{}) error {
	if bytes, err := json.Marshal(o); err != nil {
		return err
	} else {
		return b.Put([]byte(k), bytes)
	}
}

func remove(s []string, v string) []string {
	j := 0
	for _, e := range s {
		if e != v {
			s[j] = v
			j++
		}
	}
	return s[0:j]
}

// deletes the association from the implied recording (if any) to the specified tag
//
// note that the association from the tag to the recording is left unmodified.
func (ts *Service) deleteAssociation(tags *bolt.Bucket, recordings *bolt.Bucket, tag string) error {
	var existing string
	if err := read(tags, tag, &existing); err == nil {
		oldAssociation := []string{}
		if read(recordings, existing, &oldAssociation); err == nil {
			oldAssociation := remove(oldAssociation, tag)
			if len(oldAssociation) == 0 {
				if err := recordings.Delete([]byte(existing)); err != nil {
					return err
				}
			} else {
				if err := write(recordings, existing, oldAssociation); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Create or update a tag to refer to the specified id.
func (ts *Service) CreateOrUpdateTag(tag string, tagOrId string) error {

	if err := ts.ValidateTagSyntax(tag); err != nil {
		return err
	}

	if resolved, err := ts.ResolveTag(tagOrId); err != nil {
		return err
	} else {
		return ts.db.Update(func(tx *bolt.Tx) error {
			tags := tx.Bucket(tagsBucket)
			recordings := tx.Bucket(recordingsBucket)

			// if the tag already exists, then delete the existing
			// association

			if err := ts.deleteAssociation(tags, recordings, tag); err != nil {
				return nil
			}

			newAssociation := []string{}
			if read(recordings, resolved, &newAssociation); err == nil {
				// avoid duplicates
				remove(newAssociation, tag)
			}
			newAssociation = append(newAssociation, tag)

			if err := write(recordings, resolved, newAssociation); err != nil {
				return err
			}
			return write(tags, tag, resolved)
		})
	}
}

func (ts *Service) ValidateTagSyntax(tag string) error {
	if uuid_pattern.MatchString(tag) {
		return fmt.Errorf("tags may not be of the same form as recording ids")
	}

	if !tag_chars.MatchString(tag) {
		return fmt.Errorf("tags may be comprised of the characters A-Z, a-z, 0-9, _, +, - or .")
	}

	return nil
}

// Resolve a string containing a tag or id into an id.
func (ts *Service) ResolveTag(tagOrId string) (string, error) {
	if uuid_pattern.MatchString(tagOrId) {
		return tagOrId, nil
	} else {
		var id string
		err := ts.db.View(func(tx *bolt.Tx) error {
			tags := tx.Bucket(tagsBucket)
			return read(tags, tagOrId, &id)
		})
		if err == nil {
			return id, nil
		} else {
			return tagOrId, err
		}
	}
}

// Delete a tag.
func (ts *Service) DeleteTag(tag string) error {
	return ts.db.Update(func(tx *bolt.Tx) error {
		tags := tx.Bucket(tagsBucket)
		recordings := tx.Bucket(recordingsBucket)

		// if the tag already exists, then delete the existing
		// association

		if err := ts.deleteAssociation(tags, recordings, tag); err != nil {
			return nil
		}
		return tags.Delete([]byte(tag))
	})
}

// Delete a recording with the specified id and all related tags.
func (ts *Service) DeleteRecording(rid string) error {
	return ts.db.Update(func(tx *bolt.Tx) error {
		tags := tx.Bucket(tagsBucket)
		recordings := tx.Bucket(recordingsBucket)
		tagNames := []string{}
		if err := read(recordings, rid, &tagNames); err != nil {
			return err
		}
		for _, t := range tagNames {
			if err := tags.Delete([]byte(t)); err != nil {
				return err
			}
		}
		return recordings.Delete([]byte(rid))
	})
}

// List all the defined tags that match the specified pattern.
func (ts *Service) ListTags() ([]Tag, error) {
	result := []Tag{}
	if err := ts.db.View(func(tx *bolt.Tx) error {
		tags := tx.Bucket(tagsBucket)
		if err := tags.ForEach(func(k []byte, v []byte) error {
			result = append(result, Tag{Tag: string(k), ID: string(v)})
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// List all the tags for the specified recording id.
func (ts *Service) FindTagsByRecordingId(id string) ([]string, error) {
	result := []string{}
	if err := ts.db.View(func(tx *bolt.Tx) error {
		recordings := tx.Bucket(recordingsBucket)
		return read(recordings, id, &result)
	}); err != nil {
		return nil, err
	}
	return result, nil
}
