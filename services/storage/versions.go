package storage

type Versions interface {
	Get(id string) (string, error)
	Set(id, version string) error
}

type versions struct {
	store Interface
}

func NewVersions(store Interface) Versions {
	return &versions{
		store: store,
	}
}

func (v versions) Get(id string) (version string, err error) {
	err = v.store.View(func(tx ReadOnlyTx) error {
		kv, err := tx.Get(id)
		if err != nil {
			return err
		}
		version = string(kv.Value)
		return nil
	})
	return
}

func (v versions) Set(id, version string) error {
	return v.store.Update(func(tx Tx) error {
		return tx.Put(id, []byte(version))
	})
}
