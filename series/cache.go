package series

import (
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
}

type defaultCache struct {
	c *cache.Cache
}

func (dc *defaultCache) Set(k string, v interface{}) {
	dc.c.SetDefault(k, v)
}

func (dc *defaultCache) Get(k string) (interface{}, bool) {
	return dc.c.Get(k)
}

func (dc *defaultCache) Clear() {
	dc.c.Flush()
}

//InitCache
func InitCache(f func() Cache) {
	once.Do(func() {
		if f == nil {
			c = &defaultCache{
				c: cache.New(5*time.Minute, 10*time.Minute),
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
