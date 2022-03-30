package series

import (
	"strings"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
)

var c Cache

var once sync.Once

//Cache define rolling cache
type Cache interface {
	Set(k string, x interface{})
	Get(k string) (interface{}, bool)
	Clear()
	DelByKeyPrefix(keyPrefix string)
}

type defaultCache struct {
	c *cache.Cache
	keys map[string]struct{}
	mu                sync.RWMutex
}

func (dc *defaultCache) Set(k string, v interface{}) {
	err := dc.c.Add(k, v, cache.DefaultExpiration)
	if err != nil {
		dc.mu.Lock()
		dc.keys[k] = struct{}{}
		dc.mu.Unlock()
	}
}

func (dc *defaultCache) Get(k string) (interface{}, bool) {
	return dc.c.Get(k)
}

func (dc *defaultCache) Clear() {
	dc.c.Flush()
	dc.mu.Lock()
	dc.keys = map[string]struct{}{}
	dc.mu.Unlock()
}

func (dc *defaultCache) DelByKeyPrefix(keyPrefix string) {
	dc.mu.Lock()
	for key, _ := range dc.keys {
		if strings.HasPrefix(key, keyPrefix) {
			delete(dc.keys, key)
			dc.c.Delete(key)
		}
	}
	dc.mu.Unlock()
}

//InitCache
func InitCache(f func() Cache) {
	once.Do(func() {
		if f == nil {
			c = &defaultCache{
				c:    cache.New(5*time.Minute, 10*time.Minute),
				keys: map[string]struct{}{},
				mu:   sync.RWMutex{},
			}
		} else {
			c = f()
		}
	})
}

func ClearCache() {
	if c != nil {
		c.Clear()
	}
}
