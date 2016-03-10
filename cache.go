package spectre

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"strconv"
	"sync"
)

// SHARD_COUNT is the number of golang maps that are going to participate in caching
// internally.
var SHARD_COUNT int

// lowSpaceError is the error that is thrown when the cache object is capable of
// caching the given value ,but the cache object is running out of memory.
type lowSpaceError struct {
	errorNumber int
	problem string
}

func (lse *lowSpaceError) Error() string {
	return fmt.Sprintf("%d---%s", lse.errorNumber, lse.problem)
}

// sizeLimitError is the error which is thrown when the element size is larger
// than the total memory allocated to the cache object.
type sizeLimitError struct {
	errorNumber int
	problem string
}

func (sle *sizeLimitError) Error() string {
	return fmt.Sprintf("%d---%s", sle.errorNumber, sle.problem)
}
var (
	// SizeLimitError returns when size of value is greater then total allocated memory
	SizeLimitError = &sizeLimitError{problem:"data size is more than max size", errorNumber: 0}
	// LowSpaceError returns when size of the value is greater than current available space
	LowSpaceError = &lowSpaceError{problem:"space not available", errorNumber: 1}
)

// ThreadSafeMap is a thread string to interface{} map.
type ThreadSafeMap struct {
	Items        map[string]interface{}
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

func (tsm *ThreadSafeMap) String() string{
	tsm.RLocker().Lock()
	defer tsm.RLocker().Unlock()
	return fmt.Sprintf("{currentsize:%v, data:%v}", len(tsm.Items), tsm.Items)
}

// CacheData is list of threadsafe maps to participate in cache partition.
type CacheData struct {
	 MapList []*ThreadSafeMap
}

// getShardMap returns the internal map which is supposed to keep the value
// for this key. This method internally uses hashing on the key and finds out
// the internal cache map.
func (c *CacheData) getShardMap(key string) *ThreadSafeMap {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return c.MapList[uint(hasher.Sum32())%uint(SHARD_COUNT)]
}

// Cache is the stucture resposible to handle the cache key and value.
// This structure having the following parameters in it :
//			MaxSize: maximum size of the cache object
//			CurrentSize: current size of the cache object
//			Size: a golang map to store the values size key wise.
//				  this structure save the cpu cyclone to calculate he size
// 				  many time
//			Data: a pointer to the CacheData in which threadsafe maps are
// 				  storing the values
//			sync.RWMutex: to ensure the thread safety for this structure
type Cache struct {
	MaxSize int
	CurrentSize int
	Size map[string]int
	Data *CacheData
	sync.RWMutex // for atomic CurrentSize modification
}

// GetCurrentSize return the current size of the cache.
func (c *Cache) GetCurrentSize() int {
	c.RLocker().Lock()
	defer c.RLocker().Unlock()
	var totalSize int
	for _, size := range c.Size{
		totalSize = totalSize + size
	}
	return totalSize
}

func (c *Cache)String() string {
	c.RLocker().Lock()
	defer c.RLocker().Unlock()
	return fmt.Sprintf("{currentsize:%v, data:%v}", c.CurrentSize, c.Data)
}

// CacheRow is a entry in cache having structure like (key: value)
type CacheRow struct{
	Key string
	Value interface{}
}

func (c CacheRow)String() string {
	return fmt.Sprintf("cache row {key:%v, value:%v}", c.Key, c.Value)
}

// CacheIterator returns all the key and value pair  one by one in the
// input CacheRow channel. Calling goroutine  returns immediately after
// spawning a gourtine. Spawned goroutine fills the channel with CacheRow.
// When there is not any CacheRow left, spawned goroutine closes the channel.
func (c *Cache)CacheIterator(outputChannel chan CacheRow) {
	go func(){
		c.RLocker().Lock()
		defer c.RLocker().Unlock()
		for i := 0; i < SHARD_COUNT; i++ {
			for key, value := range c.Data.MapList[i].Items{
				temp := CacheRow{key, value}
				outputChannel<- temp
			}
		}
		close(outputChannel)
	}()
}

// CacheGet returns the value in the cache pointed by the
// input key parameter.
// return values :
//		ok: true if success else false
//		val: value corresponding to the key
func (c *Cache) CacheGet(key string)(interface{}, bool){
	c.RLocker().Lock()
	defer c.RLocker().Unlock()
	sharedMap := c.Data.getShardMap(key)
	sharedMap.RLocker().Lock()
	defer sharedMap.RLocker().Unlock()
	val, ok := sharedMap.Items[key]
	return val, ok
}

// CacheSet sets the key with its corresponding value in the cache.
// In case of memory unavailability, It frees some space with the defined algorithm
// and sets the key.size of the key is in bytes.
// return values :
//		success: true if success else false
//		error: error in the operation
func (c *Cache) CacheSet(key string, value interface{}, size int) (bool, error){
	success, error := c.SetData(key, value, size)
	for error == LowSpaceError{
		c.makeSpace(key, size)
		success, error = c.SetData(key, value, size)
	}
	return success, error
}

// makeSpace frees the memory to accommodate new key as given in the input
// params with its size.
// return values :
//		success: true if success else false
//		error: error in the operation
func (c *Cache)makeSpace(key string, size int) (bool, error){
	sharedMap := c.Data.getShardMap(key)
	sharedMap.RLocker().Lock()
	_, ok := sharedMap.Items[key]
	// remove the lock from current shared map
	// to avoid deadloack condition
	sharedMap.RLocker().Unlock()
	if !ok || c.Size[key] < size {
		for c.CurrentSize + size > c.MaxSize{
			c.deleteRandomKey()
		}
	}
	return true, nil
}

// deleteRandomKey deletes a random key present in the cache.
func (c *Cache) deleteRandomKey()  {
	c.Lock()
	defer c.Unlock()
	fmt.Printf("clearing space\n")
	randomNumber := strconv.Itoa(rand.Int())
	selectedMap := c.Data.getShardMap(randomNumber)
	selectedMap.Lock()
	defer selectedMap.Unlock()
	var deletedkey string
	for key, _ := range selectedMap.Items {
		delete(selectedMap.Items, key)
		deletedkey = key
		break
	}
	c.CurrentSize = c.CurrentSize - c.Size[deletedkey]
	delete(c.Size, deletedkey)
}

// isSpaceAvaible returns an boolean identifier that tells, if
// space is available for the given key.
// returns :
//		retFlag: true if space is available else false
func (c *Cache) isSpaceAvaible(key string, size int) (bool) {
	sharedMap := c.Data.getShardMap(key)
	sharedMap.RLocker().Lock()
	defer sharedMap.RLocker().Unlock()
	_, ok := sharedMap.Items[key]
	var retFlag bool
	if ok{
		if size <= c.Size[key]{
			retFlag =  true
		}else{
			retFlag =  false
		}
	}else{
		// considered a lock at cache level
		// to make thread safe
		if size <= c.MaxSize - c.CurrentSize{
			retFlag = true
		}else {
			retFlag = false
		}
	}
	return retFlag
}

func (c *Cache) isNewValueLarger(key string, size int) (bool) {
	sharedMap := c.Data.getShardMap(key)
	sharedMap.Lock()
	defer sharedMap.Unlock()
	_, ok := sharedMap.Items[key]
	var retFlag bool
	if ok && size > c.Size[key]{
			retFlag =  true
	}else{
			retFlag = true
	}
	return retFlag
}

// SetData sets a key and its corresponding value in the cache.
// This methods is responsible to set in the cache when there is other
// algorithm is applied for key eviction in case of memory unavailability.
// returns :
//		retFlag: true if space is available else false
//		error: if any error in the operation else nil
func (c *Cache) SetData(key string, value interface{}, size int) (bool, error){
	// locking currentSize atomic lock
	c.Lock()
	defer c.Unlock()
	if size > c.MaxSize{
		return false, SizeLimitError
	}else if !c.isSpaceAvaible(key, size){
		return false, LowSpaceError
	}
	sharedMap := c.Data.getShardMap(key)
	sharedMap.Lock()
	defer sharedMap.Unlock()
	sharedMap.Items[key] = value
	c.Size[key] = size
	c.CurrentSize = c.CurrentSize + size
	return true, nil
}

// CacheDelete deletes the key in the cache.
func (c *Cache)CacheDelete(key string)  {
	c.Lock()
	defer c.Unlock()
	sharedMap := c.Data.getShardMap(key);
	sharedMap.Lock()
	defer sharedMap.Unlock()
	delete(sharedMap.Items, key)
	c.CurrentSize = c.CurrentSize - int(c.Size[key])
	delete(c.Size, key)
}

// GetDefaultCache returns the most abstract cache just using the
// cap in memory limit . Cache is having algorithm to evict key when
// space is not available in random selection.
func GetDefaultCache(cacheSize int, cachePartitions int) *Cache {
	SHARD_COUNT = cachePartitions
	newCache := &Cache{
		Data: &CacheData{MapList:make([]*ThreadSafeMap, SHARD_COUNT)},
		Size: make(map[string]int),
	}

	for i := 0; i < SHARD_COUNT; i++ {
		newCache.Data.MapList[i] = &ThreadSafeMap{Items: make(map[string]interface{})}
	}
	newCache.MaxSize = cacheSize
	return newCache
}
