package storage_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/kapacitor/services/storage"
)

type object struct {
	ID    string
	Value string
	Date  time.Time
}

func (o object) ObjectID() string {
	return o.ID
}

func (o object) MarshalBinary() ([]byte, error) {
	return json.Marshal(o)
}

func (o *object) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, o)
}

func TestIndexedStore_CRUD(t *testing.T) {
	for name, sc := range stores {
		t.Run(name, func(t *testing.T) {
			db, err := sc()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			s := db.Store("crud")
			c := storage.DefaultIndexedStoreConfig("crud", func() storage.BinaryObject {
				return new(object)
			})
			c.Indexes = append(c.Indexes, storage.Index{
				Name: "date",
				ValueFunc: func(o storage.BinaryObject) (string, error) {
					obj, ok := o.(*object)
					if !ok {
						return "", storage.ImpossibleTypeErr(obj, o)
					}
					return obj.Date.UTC().Format(time.RFC3339), nil
				},
			})
			is, err := storage.NewIndexedStore(s, c)
			if err != nil {
				t.Fatal(err)
			}

			// Create new object
			o1 := &object{
				ID:    "1",
				Value: "obj1",
				Date:  time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC),
			}
			if err := is.Create(o1); err != nil {
				t.Fatal(err)
			}
			if err := is.Create(o1); err != storage.ErrObjectExists {
				t.Fatal("expected ErrObjectExists creating object1 got", err)
			}
			// Check o1
			got1, err := is.Get("1")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got1, o1) {
				t.Errorf("unexpected object 1 retrieved:\ngot\n%s\nexp\n%s\n", spew.Sdump(got1), spew.Sdump(o1))
			}
			// Check ID list
			expIDList := []storage.BinaryObject{o1}
			gotIDList, err := is.List("id", "", 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotIDList, expIDList) {
				t.Errorf("unexpected object list by ID:\ngot\n%s\nexp\n%s\n", spew.Sdump(gotIDList), spew.Sdump(expIDList))
			}
			// Check Date list
			expDateList := []storage.BinaryObject{o1}
			gotDateList, err := is.List("date", "", 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotDateList, expDateList) {
				t.Errorf("unexpected object list by Date:\ngot\n%s\nexp\n%s\n", spew.Sdump(gotDateList), spew.Sdump(expDateList))
			}

			// Create second object, using put
			o2 := &object{
				ID:    "2",
				Value: "obj2",
				Date:  time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC),
			}
			if err := is.Put(o2); err != nil {
				t.Fatal(err)
			}
			if err := is.Create(o2); err != storage.ErrObjectExists {
				t.Fatal("expected ErrObjectExists creating object2 got", err)
			}
			// Check o2
			got2, err := is.Get("2")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got2, o2) {
				t.Errorf("unexpected object 2 retrieved:\ngot\n%s\nexp\n%s\n", spew.Sdump(got2), spew.Sdump(o2))
			}
			// Check ID list
			expIDList = []storage.BinaryObject{o1, o2}
			gotIDList, err = is.List("id", "", 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotIDList, expIDList) {
				t.Errorf("unexpected object list by ID:\ngot\n%s\nexp\n%s\n", spew.Sdump(gotIDList), spew.Sdump(expIDList))
			}
			// Check Date list
			expDateList = []storage.BinaryObject{o2, o1}
			gotDateList, err = is.List("date", "", 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotDateList, expDateList) {
				t.Errorf("unexpected object list by Date:\ngot\n%s\nexp\n%s\n", spew.Sdump(gotDateList), spew.Sdump(expDateList))
			}

			// Modify objects
			o1.Value = "modified obj1"
			is.Replace(o1)
			o2.Date = time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
			is.Put(o2)

			// Check o1
			got1, err = is.Get("1")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got1, o1) {
				t.Errorf("unexpected object 1 retrieved after modification:\ngot\n%s\nexp\n%s\n", spew.Sdump(got1), spew.Sdump(o1))
			}

			// Check o2
			got2, err = is.Get("2")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got2, o2) {
				t.Errorf("unexpected object 2 retrieved after modification:\ngot\n%s\nexp\n%s\n", spew.Sdump(got2), spew.Sdump(o2))
			}

			// Check ID list
			expIDList = []storage.BinaryObject{o1, o2}
			gotIDList, err = is.List("id", "", 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotIDList, expIDList) {
				t.Errorf("unexpected object list by ID after modification:\ngot\n%s\nexp\n%s\n", spew.Sdump(gotIDList), spew.Sdump(expIDList))
			}
			// Check Date list
			expDateList = []storage.BinaryObject{o1, o2}
			gotDateList, err = is.List("date", "", 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotDateList, expDateList) {
				t.Errorf("unexpected object list by Date after modification:\ngot\n%s\nexp\n%s\n", spew.Sdump(gotDateList), spew.Sdump(expDateList))
			}

			// Delete object 2
			if err := is.Delete("2"); err != nil {
				t.Fatal(err)
			}

			// Check o2
			if _, err := is.Get("2"); err != storage.ErrNoObjectExists {
				t.Error("expected ErrNoObjectExists for delete object 2, got:", err)
			}

			// Check ID list
			expIDList = []storage.BinaryObject{o1}
			gotIDList, err = is.List("id", "", 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotIDList, expIDList) {
				t.Errorf("unexpected object list by ID after modification:\ngot\n%s\nexp\n%s\n", spew.Sdump(gotIDList), spew.Sdump(expIDList))
			}
			// Check Date list
			expDateList = []storage.BinaryObject{o1}
			gotDateList, err = is.List("date", "", 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotDateList, expDateList) {
				t.Errorf("unexpected object list by Date after modification:\ngot\n%s\nexp\n%s\n", spew.Sdump(gotDateList), spew.Sdump(expDateList))
			}

			// Try to replace non existent object
			o3 := &object{
				ID:    "3",
				Value: "obj3",
				Date:  time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC),
			}
			if err := is.Replace(o3); err != storage.ErrNoObjectExists {
				t.Error("expected error replacing non existent object, got:", err)
			}
		})
	}
}
