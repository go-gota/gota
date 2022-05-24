package series

import (
	"sync"
)

//Cache define series cache
type Cache interface {
	Set(key string, value interface{})
	Get(key string) (interface{}, bool)
	Clear()
	Size() int
	Delete(key string)
	Copy() Cache
}

type seriesCache struct {
	c  map[string]interface{}
	mu sync.RWMutex
}

func newSeriesCache() Cache {
	ch := &seriesCache{
		c:  map[string]interface{}{},
		mu: sync.RWMutex{},
	}
	return ch
}

func (dc *seriesCache) Set(key string, value interface{}) {
	dc.mu.Lock()
	dc.c[key] = value
	dc.mu.Unlock()
}

func (dc *seriesCache) Size() int {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	return len(dc.c)
}

func (dc *seriesCache) Get(key string) (interface{}, bool) {
	dc.mu.RLock()
	v, ok := dc.c[key]
	dc.mu.RUnlock()
	return v, ok
}

func (dc *seriesCache) Clear() {
	dc.mu.Lock()
	dc.c = make(map[string]interface{})
	dc.mu.Unlock()
}

func (dc *seriesCache) Delete(key string) {
	dc.mu.Lock()
	delete(dc.c, key)
	dc.mu.Unlock()
}

func (dc *seriesCache) Copy() Cache {
	nc := &seriesCache{
		c:  map[string]interface{}{},
		mu: sync.RWMutex{},
	}
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	for k, v := range dc.c {
		switch vt := v.(type) {
		case Series:
			nc.c[k] = vt.Copy()
		case Element:
			nc.c[k] = vt.Copy()
		case string, float64, int, bool:
			nc.c[k] = vt
		default:
			nc.c[k] = vt
		}
	}
	return nc
}
