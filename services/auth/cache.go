package auth

import (
	"sync"
	"time"

	"github.com/influxdata/kapacitor/auth"
)

type UserCache interface {
	Get(username string) (auth.User, bool)
	Set(auth.User)
	Delete(username string)
	DeleteAll()
}

// In memory based implementation of User cache
// Lazily expires items from the cache.
// All methods are goroutine safe.
type memUserCache struct {
	mu         sync.RWMutex
	cache      map[string]cacheItem
	expiration time.Duration
}

type cacheItem struct {
	User       auth.User
	Expiration time.Time
}

func newMemUserCache(expiration time.Duration) *memUserCache {
	return &memUserCache{
		cache:      make(map[string]cacheItem),
		expiration: expiration,
	}
}

func (c *memUserCache) Get(username string) (auth.User, bool) {
	c.mu.RLock()
	item, ok := c.cache[username]
	c.mu.RUnlock()
	if item.Expiration.Before(time.Now()) {
		c.mu.Lock()
		delete(c.cache, username)
		c.mu.Unlock()
		ok = false
	}
	return item.User, ok
}

func (c *memUserCache) Set(u auth.User) {
	c.mu.Lock()
	c.cache[u.Name()] = cacheItem{
		User:       u,
		Expiration: time.Now().Add(c.expiration),
	}
	c.mu.Unlock()
}

func (c *memUserCache) Delete(username string) {
	c.mu.Lock()
	delete(c.cache, username)
	c.mu.Unlock()
}

func (c *memUserCache) DeleteAll() {
	c.mu.Lock()
	for username := range c.cache {
		delete(c.cache, username)
	}
	c.mu.Unlock()
}
