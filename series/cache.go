package series

import (
	"strings"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
)

var c Cache

var once sync.Once

//Cache define series cache
type Cache interface {
	Set(k string, x interface{})
	Get(k string) (interface{}, bool)
	Clear()
	DelByKeyPrefix(keyPrefix string) int
	Size() int
}

type seriesCache struct {
	c    *cache.Cache
	keys map[string]struct{}
	mu   sync.RWMutex
}

func NewDefaultCache() Cache {
	ch := &seriesCache{
		c:    cache.New(5*time.Minute, 10*time.Minute),
		keys: map[string]struct{}{},
		mu:   sync.RWMutex{},
	}
	return ch
}

func (dc *seriesCache) Set(k string, v interface{}) {
	err := dc.c.Add(k, v, cache.DefaultExpiration)
	if err == nil {
		dc.mu.Lock()
		dc.keys[k] = struct{}{}
		dc.mu.Unlock()
	}
}

func (dc *seriesCache) Size() int { 
	return dc.c.ItemCount()
}

func (dc *seriesCache) Get(k string) (interface{}, bool) {
	return dc.c.Get(k)
}

func (dc *seriesCache) Clear() {
	dc.c.Flush()
	dc.mu.Lock()
	dc.keys = map[string]struct{}{}
	dc.mu.Unlock()
}

func (dc *seriesCache) DelByKeyPrefix(keyPrefix string) int {
	delCount := 0
	dc.mu.Lock()
	for key := range dc.keys {
		if strings.HasPrefix(key, keyPrefix) {
			delete(dc.keys, key)
			dc.c.Delete(key)
			delCount++
		}
	}
	dc.mu.Unlock()
	return delCount
}

//InitCache
func InitCache(f func() Cache) {
	once.Do(func() {
		if f == nil {
			c = NewDefaultCache()
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
