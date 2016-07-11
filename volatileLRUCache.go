package spectre

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Link is a node in circular doubly linked list that stores information about the
// key usage and time to live
// structure is like :
//
//
// link1 prev\				  /link2 next
//			link1		   link2
//	 link1 next\        	/link2 prev
//				\		   /
//				 \		  /
//	    root prev \      /root next
//				    ROOT
//
type Link struct {
	key        string
	ExpireTime time.Time
	size       int
	ttlPrev    *Link
	ttlNext    *Link
	lruPrev    *Link
	lruNext    *Link
}

// isLinkTTLExpired tells in boolean about the key expiration.
// true if expired or false.
func (l *Link) isLinkTTLExpired() bool {
	//fmt.Printf("current time= %v \n", time.Now())
	//fmt.Printf("local time = %v \n", l.ExpireTime)
	//fmt.Printf("local expired = %v \n", l.ExpireTime.Before(time.Now()))
	return l.ExpireTime.Before(time.Now())
}

// addLRULink adds a lru link in the circular doubly link list between root
// and a node left of it.
func (l *Link) addLRULink(temp *Link) {
	l.lruNext = temp
	l.lruPrev = temp.lruPrev
	temp.lruPrev.lruNext = l
	temp.lruPrev = l
}

// addLRULink adds a ttl link in the circular doubly link list between root
// and a node left of it.
func (l *Link) addTTLLink(temp *Link) {
	l.ttlNext = temp
	l.ttlPrev = temp.ttlPrev
	temp.ttlPrev.ttlNext = l
	temp.ttlPrev = l
}

// unlinkLRULink unlinks the link from its lru pointers in the
// doubly link list
func (temp *Link) unlinkLRULink() {
	nextLink := temp.lruNext
	prevLink := temp.lruPrev
	nextLink.lruPrev = prevLink
	prevLink.lruNext = nextLink
}

// unlinkTTLLink unlinks the link from its ttl pointers in the
// doubly link list
func (temp *Link) unlinkTTLLink() {
	nextLink := temp.ttlNext
	prevLink := temp.ttlPrev
	nextLink.ttlPrev = prevLink
	prevLink.ttlNext = nextLink
}

// unlink removes a link in circular doubly link list.
func (temp *Link) unlink() {
	temp.unlinkLRULink()
	temp.unlinkTTLLink()
}

// add insert a new link in circular doubly link list
func (l *Link) add(temp *Link) {
	l.addLRULink(temp)
	l.addTTLLink(temp)
}

// VolatileLRUCache is a cache wrapper on top of Cache.
// so still the maximum size of the cache is controlled
// by Cache only. This wrapper just adds an algorithm for
// key eviction policy .
// In case of memory unavailability VolatileLRUCache deletes
// the keys in the following order :
// 		*** keys which has been expired then the keys which are
//		*** keys which are least recently used
// VolatileLRUCache maintains a circular doubly link list in memory
// to have the meta data of the keys ready. Also this structure is
// thread safe; meaning several goroutine can operate concurrently.
type VolatileLRUCache struct {
	cache        *Cache
	root         *Link
	linkMap      map[string]*Link
	globalTTL    time.Duration
	sync.RWMutex // to make double linked list thread safe
}

// GetCurrentSize is a wrapper on top of Cache GetCurrentSize
// which returns the current VolatileLRUCache size in bytes.
func (vlruCache *VolatileLRUCache) VolatileLRUCacheCurrentSize() int {
	vlruCache.RLocker().Lock()
	defer vlruCache.RLocker().Unlock()
	return vlruCache.cache.GetCurrentSize()
}

func (vlruCache *VolatileLRUCache) String() string {
	vlruCache.RLocker().Lock()
	defer vlruCache.RLocker().Unlock()
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("currentsize:%v\n", vlruCache.cache.CurrentSize))
	buffer.WriteString(vlruCache.GetLRUInfo())
	buffer.WriteString(vlruCache.GetTTLInfo())
	return buffer.String()
}

// GetLRUInfo return the lru information of the keys in VolatileLRUCache.
func (vlruCache *VolatileLRUCache) GetLRUInfo() string {
	vlruCache.RLocker().Lock()
	defer vlruCache.RLocker().Unlock()
	rootLink := vlruCache.root
	startingLink := rootLink.lruNext
	var keyList []string
	for startingLink != rootLink {
		keyList = append(keyList, startingLink.key)
		nextLink := startingLink.lruNext
		startingLink = nextLink
	}
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("key order in lru fashion with old first stratgy\n"))
	for i, key := range keyList {
		buffer.WriteString(fmt.Sprintf("{position:%v, key:%v}\t", i, key))
	}
	return buffer.String()
}

// GetLRUInfo return the ttl information of the keys in VolatileLRUCache.
func (vlruCache *VolatileLRUCache) GetTTLInfo() string {
	vlruCache.RLocker().Lock()
	defer vlruCache.RLocker().Unlock()
	rootLink := vlruCache.root
	startingLink := rootLink.ttlNext
	var keyList []string
	for startingLink != rootLink {
		keyList = append(keyList, startingLink.key)
		nextLink := startingLink.ttlNext
		startingLink = nextLink
	}
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("key order in ttl fashion with old first stratgy\n"))
	for i, key := range keyList {
		buffer.WriteString(fmt.Sprintf("{position:%v, key:%v}\t", i, key))
	}
	return buffer.String()
}

func (vlruCache *VolatileLRUCache) VolatileLRUCacheIterator(outputChannel chan CacheRow) {
	go func() {
		//panic handlling at goroutine level
		defer func() {
			if r := recover(); r != nil {
				close(outputChannel)
			}
		}()

		vlruCache.RLocker().Lock()
		defer vlruCache.RLocker().Unlock()
		rootLink := vlruCache.root
		// taking ttl pointer to start with the oldest key
		startingLink := rootLink.ttlNext
		for startingLink != rootLink {
			if !startingLink.isLinkTTLExpired() {
				val, ok := vlruCache.cache.CacheGet(startingLink.key)
				if ok {
					outputChannel <- CacheRow{Key: startingLink.key, Value: val}
				}
			}
			nextLink := startingLink.ttlNext
			startingLink = nextLink
		}
		close(outputChannel)
		//fmt.Printf("spawaned go routine finishes")
	}()
}

// VolatileLRUCacheGet returns the value corresponding to a key present in Cache.
// This also modify internal doubly link list to maintain the updated ttl and lru info
// of the keys preset in Cache.
// return values :
//		value: value corresponding to the key
//		ok: true if success else false
func (vlruCache *VolatileLRUCache) VolatileLRUCacheGet(key string) (interface{}, bool) {
	// lower level is thread safe so making write lock after this.
	value, ok := vlruCache.cache.CacheGet(key)
	//fmt.Printf("cache value = %v error is = %v" , value, ok)
	// changing the link so grabbing write lock
	vlruCache.Lock()
	defer vlruCache.Unlock()
	keyLink := vlruCache.linkMap[key]
	if !ok || keyLink.isLinkTTLExpired() {
		return nil, false
	}
	keyLink.unlinkLRULink()
	keyLink.addLRULink(vlruCache.root)
	return value, ok
}

// VolatileLRUCacheSet sets the value corresponding to a key in Cache.
// Setting operation also removes the keys which are already expired ; so as to
// make the rem free as much as possible. In case of memory is not available
// even after removing expired keys it removes the lru keys.
// This also modify internal doubly link list to maintain the updated ttl and lru info
// of the keys preset in Cache.
//
// input params :
//				key: key to hold the value in cache (string type)
//				value: struct having the data to cache.
//				keyExpire: time duration for the current key expire.
// return values :
//		ok: true if operation is successful else false
//		error: error in case of occurred error else nil
func (vlruCache *VolatileLRUCache) VolatileLRUCacheSet(key string, value interface{}, size int, keyExpire time.Duration) (bool, error) {
	//free memory from expired keys
	vlruCache.Lock()
	defer vlruCache.Unlock()
	vlruCache.RemoveVolatileKey()
	success, error := vlruCache.cache.SetData(key, value, size)
	for error == LowSpaceError {
		vlruCache.makeSpace()
		success, error = vlruCache.cache.SetData(key, value, size)
	}
	if !success {
		return success, error
	}
	link, ok := vlruCache.linkMap[key]
	if !ok {
		link = &Link{}
		vlruCache.linkMap[key] = link
	} else {
		link.unlink()
	}
	link.key = key
	if keyExpire.Seconds() <= 0 {
		link.ExpireTime = time.Now().Add(vlruCache.globalTTL)
	} else {
		link.ExpireTime = time.Now().Add(keyExpire)
	}
	link.size = size
	link.add(vlruCache.root)
	return true, nil
}

// RemoveVolatileKey removes the keys which are already expired in VolatileLRUCache.
func (vlruCache *VolatileLRUCache) RemoveVolatileKey() {
	rootLink := vlruCache.root
	startingLink := rootLink.ttlNext
	for startingLink != rootLink && startingLink.isLinkTTLExpired() {
		vlruCache.cache.CacheDelete(startingLink.key)
		delete(vlruCache.linkMap, startingLink.key)
		nextLink := startingLink.ttlNext
		startingLink.unlink()
		startingLink = nextLink
		// to free memory # golang garbage collector
		//runtime.GC()
	}
}

// VolatileLRUCacheDelete deletes a key present in VolatileLRUCache.
func (vlruCache *VolatileLRUCache) VolatileLRUCacheDelete(key string) {
	// lower level is thread safe so making write lock after this.
	_, ok := vlruCache.cache.CacheGet(key)
	if ok {
		// changing the link so grabbing write lock
		vlruCache.Lock()
		defer vlruCache.Unlock()
		vlruCache.RemoveVolatileKey()
		vlruCache.cache.CacheDelete(key)
		deletedLink := vlruCache.linkMap[key]
		if deletedLink != nil {
			deletedLink.unlink()
			delete(vlruCache.linkMap, key)
		}
	}
}

// makeSpace frees the space with least recently key.
// return values :
//		ok: true if operation is successful else false
//		error: error in case of occurred error else nil
func (vlruCache *VolatileLRUCache) makeSpace() (bool, error) {
	fmt.Printf("clearing space in volatile cache\n")
	// linkTBE means link to be evicted with its data(key, value) in cache
	linkTBE := vlruCache.root.lruNext
	if linkTBE == vlruCache.root {
		return false, errors.New("VolatileLRUCache is empty ... May be the memory is less")
	}
	key := linkTBE.key
	vlruCache.cache.CacheDelete(key)
	linkTBE.unlink()
	delete(vlruCache.linkMap, key)
	return true, nil
}

// VolatileLRUCacheClear clears all the keys in the cache.
func (vlruCache *VolatileLRUCache) VolatileLRUCacheClear() {
	vlruCache.Lock()
	defer vlruCache.Unlock()
	vlruCache.cache.ClearCache()
	vlruCache.root = &Link{}
	vlruCache.linkMap = make(map[string]*Link)
	vlruCache.root.lruNext = vlruCache.root
	vlruCache.root.lruPrev = vlruCache.root
	vlruCache.root.ttlNext = vlruCache.root
	vlruCache.root.ttlPrev = vlruCache.root
}

// GetVolatileLRUCache returns an instance of VolatileLRUCache with the specified
// input params:
//			cacheSize: size of the cache in bytes
//			cachePartitions: total number map participating in internal cache.
//			ttl: a global time duration for each key expiration.
func GetVolatileLRUCache(cacheSize int, cachePartitions int, ttl time.Duration) *VolatileLRUCache {
	newVolatileCache := &VolatileLRUCache{
		cache:   GetDefaultCache(cacheSize, cachePartitions),
		root:    &Link{},
		linkMap: make(map[string]*Link),
	}
	//converting ttl to seconds for microseconds
	ttl = ttl * time.Second
	newVolatileCache.globalTTL = ttl
	//fmt.Printf("global ttl  .... %v \n", newVolatileCache.globalTTL)
	//fmt.Printf("current time.... %v \n", time.Now())
	newVolatileCache.root.lruNext = newVolatileCache.root
	newVolatileCache.root.lruPrev = newVolatileCache.root
	newVolatileCache.root.ttlNext = newVolatileCache.root
	newVolatileCache.root.ttlPrev = newVolatileCache.root
	return newVolatileCache
}
