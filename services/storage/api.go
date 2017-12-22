package storage

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/boltdb/bolt"
	client "github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/services/httpd"
)

const (
	storagePath            = "/storage"
	storagePathAnchored    = storagePath + "/"
	backupPath             = storagePath + "/backup"
	storesPath             = storagePath + "/stores"
	storesPathAnchored     = storesPath + "/"
	storesBasePath         = httpd.BasePath + storesPath
	storesBasePathAnchored = httpd.BasePath + storesPathAnchored
)

type APIServer struct {
	Registrar StoreActionerRegistrar
	DB        *bolt.DB
	routes    []httpd.Route
	diag      Diagnostic

	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
}

func (s *APIServer) Open() error {
	s.routes = []httpd.Route{
		{
			Method:      "GET",
			Pattern:     backupPath,
			HandlerFunc: s.handleBackup,
			// Do not gzip the data so that Content-Length is preserved.
			NoGzip: true,
			NoJSON: true,
		},
		{
			Method:      "GET",
			Pattern:     storesPath,
			HandlerFunc: s.handleListStores,
		},
		{
			Method:      "POST",
			Pattern:     storagePathAnchored,
			HandlerFunc: s.handleStoreAction,
		},
	}
	err := s.HTTPDService.AddRoutes(s.routes)
	if err != nil {
		return err
	}
	return nil
}

func (s *APIServer) Close() error {
	s.HTTPDService.DelRoutes(s.routes)
	return nil
}

func (s *APIServer) handleBackup(w http.ResponseWriter, r *http.Request) {
	err := s.DB.View(func(tx *bolt.Tx) error {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="kapacitor.db"`)
		w.Header().Set("Content-Length", strconv.Itoa(int(tx.Size())))
		w.WriteHeader(http.StatusOK)
		_, err := tx.WriteTo(w)
		return err
	})
	if err != nil {
		// We only log the error since we can't send it to the client
		// since the headers have already been sent.
		// The client can simply check that the Content-Length matches
		// the amount of data to ensure a successful backup was performed.
		s.diag.Error("failed to send backup data", err)
	}
}

func (s *APIServer) handleListStores(w http.ResponseWriter, r *http.Request) {
	storages := s.Registrar.List()
	list := client.StorageList{
		Link:    client.Link{Relation: client.Self, Href: storesBasePath},
		Storage: make([]client.Storage, len(storages)),
	}
	for i, name := range storages {
		list.Storage[i] = client.Storage{
			Link: client.Link{Relation: client.Self, Href: path.Join(storesBasePath, name)},
			Name: name,
		}
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(list, true))
}

func (s *APIServer) handleStoreAction(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, storesBasePathAnchored)
	store, ok := s.Registrar.Get(name)
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("unknown storage %q", name), true, http.StatusNotFound)
		return
	}

	action := client.StorageActionOptions{}
	err := json.NewDecoder(r.Body).Decode(&action)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to unmarshal storage action %v", err), true, http.StatusBadRequest)
		return
	}

	switch action.Action {
	case client.StorageRebuild:
		err := store.Rebuild()
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("failed to rebuild %q: %v", name, err), true, http.StatusInternalServerError)
			return
		}
	default:
		httpd.HttpError(w, fmt.Sprintf("unkown storage action %q", action.Action), true, http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
