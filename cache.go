package spectre


import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"strconv"
	"sync"
)

var SHARD_COUNT int

type lowSpaceError struct {
	errorNumber int
	problem string
}

func (lse *lowSpaceError) Error() string {
	return fmt.Sprintf("%d---%s", lse.errorNumber, lse.problem)
}

type sizeLimitError struct {
	errorNumber int
	problem string
}

func (sle *sizeLimitError) Error() string {
	return fmt.Sprintf("%d---%s", sle.errorNumber, sle.problem)
}
var (
	SizeLimitError = &sizeLimitError{problem:"data size is more than max size", errorNumber: 0}
	LowSpaceError = &lowSpaceError{problem:"space not available", errorNumber: 1}
)

// A list of maps to support cache partition for threadsafe behaviour
type CacheData struct {
	 MapList []*ThreadSafeMap
}

func (c *CacheData) getShardMap(key string) *ThreadSafeMap {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return c.MapList[uint(hasher.Sum32())%uint(SHARD_COUNT)]
}

// A thread safe string to []byte map.
type ThreadSafeMap struct {
	Items        map[string][]byte
	sync.RWMutex // Read Write mutex, guards access to internal map.
	//sync.Mutex
}

func (tsm *ThreadSafeMap) String() string{
	return fmt.Sprintf("{currentsize:%v, data:%v}", len(tsm.Items), tsm.Items)
}

type Cache struct {
	//CacheMode bool to be added later
	MaxSize int
	CurrentSize int
	Size map[string]int
	Data *CacheData
}

func (c *Cache) GetCurrentSize() int {
	var totalSize int
	for _, size := range c.Size{
		totalSize = totalSize + size
	}
	return totalSize
}


func (c *Cache)String() string {
	return fmt.Sprintf("{currentsize:%v, data:%v}", c.CurrentSize, c.Data)
}


func (c *Cache)CacheIterator(outputChannel chan map[string][]byte ) {
	for i := 0; i < SHARD_COUNT; i++ {
		 outputChannel <- c.Data.MapList[i].Items
	}
}

func (c *Cache) CacheGet(key string)([]byte, bool){
	sharedMap := c.Data.getShardMap(key)
	sharedMap.Lock()
	defer sharedMap.Unlock()
	val, ok := sharedMap.Items[key]
	return val, ok
}

func (c *Cache) CacheSet(key string, value []byte, size int) (bool, error){
	success, error := c.SetData(key, value, size)
	if !success && error == LowSpaceError {
		c.makeSpace(key, size)
		success, error = c.SetData(key, value, size)
	}
	return success, error
}

func (c *Cache)makeSpace(key string, size int) (bool, error){
	sharedMap := c.Data.getShardMap(key)
	sharedMap.Lock()
	_, ok := sharedMap.Items[key]
	// remove the lock from current shared map
	// to avoid deadloack condition
	sharedMap.Unlock()
	if !ok || c.Size[key] < size {
		for c.CurrentSize + size > c.MaxSize{
			c.deleteRandomKey()
		}
	}
	return true, nil
}

func (c *Cache) deleteRandomKey()  {
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

func (c *Cache) isSpaceAvaible(key string, size int) (bool) {
	sharedMap := c.Data.getShardMap(key)
	sharedMap.Lock()
	defer sharedMap.Unlock()
	_, ok := sharedMap.Items[key]
	var retFlag bool
	if ok{
		if size <= c.Size[key]{
			retFlag =  true
		}else{
			retFlag =  false
		}
	}else{
		// consider a lock at cache level
		// to make thread safe
		if size <= c.MaxSize - c.CurrentSize{
			retFlag = true
		}else {
			retFlag = false
		}
	}
	return retFlag
}

func (c *Cache) SetData(key string, value []byte, size int) (bool, error){
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

func (c *Cache)CacheDelete(key string)  {
	sharedMap := c.Data.getShardMap(key);
	sharedMap.Lock()
	defer sharedMap.Unlock()
	delete(sharedMap.Items, key)
	c.CurrentSize = c.CurrentSize - int(c.Size[key])
	delete(c.Size, key)
}

func GetDefaultCache(cacheSize int, cachePartitions int) *Cache {
	newCache := &Cache{
		Data: &CacheData{MapList:make([]*ThreadSafeMap, cachePartitions)},
		Size: make(map[string]int),
	}

	for i := 0; i < cachePartitions; i++ {
		newCache.Data.MapList[i] = &ThreadSafeMap{Items: make(map[string][]byte)}
	}
	newCache.MaxSize = cacheSize
	SHARD_COUNT = cachePartitions
	return newCache
}
