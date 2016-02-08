package spectre

import (
	"fmt"
	"time"
	"bytes"
	"errors"
)

// link is a node in circular doubly linked list that stores information about the
// key usage and time to live
// structure like :
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
//
type Link struct {
	key string
	ExpireTime time.Time
	size int
	ttlPrev *Link
	ttlNext *Link
	lruPrev *Link
	lruNext *Link
}

func (l *Link)isLinkTTLExpired()(bool)  {
	fmt.Printf("current time= %v \n", time.Now())
	fmt.Printf("local time = %v \n", l.ExpireTime)
	fmt.Printf("local expired = %v \n", l.ExpireTime.Before(time.Now()))
	return l.ExpireTime.Before(time.Now())
}

func (l *Link)addLRULink(temp *Link )  {
	l.lruNext = temp
	l.lruPrev = temp.lruPrev
	temp.lruPrev.lruNext = l
	temp.lruPrev = l
}

func (l *Link)addTTLLink(temp *Link )  {
	l.ttlNext = temp
	l.ttlPrev = temp.ttlPrev
	temp.ttlPrev.ttlNext = l
	temp.ttlPrev = l
}

func (temp *Link) unlinkLRULink()  {
	nextLink := temp.lruNext
	prevLink := temp.lruPrev
	nextLink.lruPrev = prevLink
	prevLink.lruNext = nextLink
}

func (temp *Link) unlinkTTLLink()  {
	nextLink := temp.ttlNext
	prevLink := temp.ttlPrev
	nextLink.ttlPrev = prevLink
	prevLink.ttlNext = nextLink
}

func (temp *Link) unlink()  {
	temp.unlinkLRULink()
	temp.unlinkTTLLink()
}

func (l *Link) add(temp *Link)  {
	l.addLRULink(temp)
	l.addTTLLink(temp)
}

type VolatileLRUCache struct {
	cache *Cache
	root *Link
	linkMap map[string]*Link
	globalTTL time.Duration
}

func (vlruCache *VolatileLRUCache) GetCurrentSize() int {
	return vlruCache.cache.GetCurrentSize()
}

func  (vlruCache *VolatileLRUCache) String() string  {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("currentsize:%v\n", vlruCache.cache.CurrentSize))
	buffer.WriteString(vlruCache.GetLRUInfo())
	buffer.WriteString(vlruCache.GetTTLInfo())
	return buffer.String()
}

func (vlruCache *VolatileLRUCache) GetLRUInfo() string {
	rootLink := vlruCache.root
	startingLink := rootLink.lruNext
	var keyList []string
	for ;startingLink != rootLink; {
		keyList = append(keyList, startingLink.key)
		nextLink := startingLink.lruNext
		startingLink = nextLink
	}
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("key order in lru fashion with old first stratgy\n"))
	for i ,key := range keyList{
		buffer.WriteString(fmt.Sprintf("{position:%v, key:%v}\t", i, key))
	}
	return buffer.String()
}

func (vlruCache *VolatileLRUCache) GetTTLInfo() string {
	rootLink := vlruCache.root
	startingLink := rootLink.ttlNext
	var keyList []string
	for ;startingLink != rootLink; {
		keyList = append(keyList, startingLink.key)
		nextLink := startingLink.ttlNext
		startingLink = nextLink
	}
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("key order in ttl fashion with old first stratgy\n"))
	for i ,key := range keyList{
		buffer.WriteString(fmt.Sprintf("{position:%v, key:%v}\t", i, key))
	}
	return buffer.String()
}

func (vlruCache *VolatileLRUCache)VolatileLRUCacheIterator(outputChannel chan []byte ) {
	rootLink := vlruCache.root
	startingLink := rootLink.ttlNext
	for ;startingLink != rootLink; {
		if !startingLink.isLinkTTLExpired(){
			val, _ := vlruCache.cache.CacheGet(startingLink.key)
			outputChannel <- val
		}
		nextLink := startingLink.ttlNext
		startingLink = nextLink
	}
}

func (vlruCache *VolatileLRUCache) VolatileLRUCacheGet(key string)([]byte, bool){
	value, ok := vlruCache.cache.CacheGet(key)
	fmt.Printf("cache value = %v error is = %v" , string(value), ok)
	keyLink := vlruCache.linkMap[key]
	if !ok || keyLink.isLinkTTLExpired(){
		return nil, false
	}
	keyLink.unlinkLRULink()
	keyLink.addLRULink(vlruCache.root)
	return value, ok
}

func (vlruCache *VolatileLRUCache) VolatileLRUCacheSet(key string, value []byte, size int, keyExpire time.Duration) (bool, error){
	//free memory from expired keys
	vlruCache.RemoveVolatileKey()
	success, error := vlruCache.cache.SetData(key, value, size)
	if  !success && error == LowSpaceError{
		vlruCache.makeSpace()
		success, error := vlruCache.cache.SetData(key, value, size)
		if !success{
			return success, error
		}
	}
	link, ok := vlruCache.linkMap[key]
	if !ok{
		link = &Link{}
		vlruCache.linkMap[key] = link
	}
	link.key = key
	if keyExpire.Seconds() <= 0{
		link.ExpireTime = time.Now().Add(vlruCache.globalTTL)
	}else{
		link.ExpireTime = time.Now().Add(keyExpire)
	}
	link.size = size
	link.add(vlruCache.root)
	return true, nil
}

func(vlruCache *VolatileLRUCache) RemoveVolatileKey() {
	rootLink := vlruCache.root
	startingLink := rootLink.ttlNext
	for ;startingLink != rootLink && startingLink.isLinkTTLExpired(); {
		vlruCache.cache.CacheDelete(startingLink.key)
		delete(vlruCache.linkMap, startingLink.key)
		nextLink := startingLink.ttlNext
		startingLink.unlink()
		startingLink = nextLink
		// to free memory # golang garbage collector
		//runtime.GC()
	}
}

func (vlruCache *VolatileLRUCache)VolatileLRUCacheDelete(key string)  {
	vlruCache.RemoveVolatileKey()
	vlruCache.cache.CacheDelete(key)
	deletedLink := vlruCache.linkMap[key]
	deletedLink.unlink()
	delete(vlruCache.linkMap, key)
}

func (vlruCache *VolatileLRUCache)makeSpace() (bool, error){
	fmt.Printf("clearing space in volatile cache\n")
	// linkTBE means link to be evicted with its data(key, value) in cache
	linkTBE := vlruCache.root.lruNext
	if linkTBE == vlruCache.root{
		return false, errors.New("volatileLRUcache is empty ... May be the memory is less")
	}
	key := linkTBE.key
	vlruCache.cache.CacheDelete(key)
	linkTBE.unlink()
	delete(vlruCache.linkMap, key)
	return true, nil
}

func GetVolatileLRUCache(cacheSize int, cachePartitions int, ttl time.Duration) *VolatileLRUCache {
	newVolatileCache := &VolatileLRUCache{
		cache: GetDefaultCache(cacheSize, cachePartitions),
		root: &Link{},
		linkMap: make(map[string]*Link),
	}
	//converting ttl to seconds for microseconds
	ttl = ttl * time.Second
	newVolatileCache.globalTTL = ttl
	fmt.Printf("global ttl  .... %v \n", newVolatileCache.globalTTL)
	fmt.Printf("current time.... %v \n", time.Now())
	newVolatileCache.root.lruNext = newVolatileCache.root
	newVolatileCache.root.lruPrev = newVolatileCache.root
	newVolatileCache.root.ttlNext = newVolatileCache.root
	newVolatileCache.root.ttlPrev = newVolatileCache.root
	return newVolatileCache
}
