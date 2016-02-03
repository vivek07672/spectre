<img src="spectre.png" width="300">
# spectre
Dont fear ... its a friendly ghost . Spectre is a goroutine safe process level cache following LRU pattern with ttl (time to live) for each key.
When the memory gets fully utilised , the key which has been used before a long time is evicted . Also there is a cleanup methods for expired keys at the time of SET and DELETE actions . This cleanup ensure to keep the memory free from expired keys for other process.
In web servers there is lots of memory availble at cheaper cost which can be utilised in optimum manner to get the right benefit.
Spectre caches the serialised data for a string key which can save a same next heavy cpu based computation or a network call. 


##How to install:

```bash
go get github.com/vivek07672/spectre
```

##How to use:

```go
...

import "github.com/vivek07672/spectre"

// so the first step is to get the cache variable
volatileLRUCache := volatileLRUCache.GetVolatileLRUCache(cacheSize int, cachePartitions int, ttl time.Duration)
the parameters :
	cacheSize: size of ram memory in bytes that is to be allocated to the cache
	cachePartitions: no of internal maps to be used
				- larger the number better thread safe (32 is a good count )
			 inside total this many maps are kept to cache the date , each map can give access to 
			 one goroutine at any time .
	ttl: a global timeout for all the keys
	
// now its time for some operation testing
// SETTING IN CACHE
	fmt.Print("Enter the key: \n")
	fmt.Scanf("%s", &key)
	fmt.Print("Enter the value: \n")
	fmt.Scanf("%s", &value)
	serialisedValue := []byte(value)
	size = int(binary.Size(serialisedValue))
	// here time.Duration(0) is setting the key level expire ; 0 is duration seconds after which key gets expired 
	volatileLRUCache.VolatileLRUCacheSet(key, serialisedValue, size, time.Duration(0))

Note : local exire has priority over global expiry . 

// GETTING FROM CACHE
	var key string
	fmt.Print("Enter the key: \n")
	fmt.Scanf("%s", &key)
	val, ok := volatileLRUCache.VolatileLRUCacheGet(key)
	fmt.Printf("value = %v and ok = %v\n", string(val), ok)
	fmt.Printf("type of value is = %T", val)

// DELETING FROM CACHE
	var key string
	fmt.Print("Enter the key: \n")
	fmt.Scanf("%s", &key)
	volatileLRUCache.VolatileLRUCacheDelete(key)
...
```
