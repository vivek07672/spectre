package spectre

import (
	"encoding/binary"
	"reflect"
	"testing"
	"time"
)

var VolatileLRUCacheTest = GetVolatileLRUCache(50000.0, 15, time.Duration(3600))

func TestVolatileLRUCacheCurrentSize(t *testing.T) {
	var size = 0
	size = size + binary.Size([]byte("vivek"))
	VolatileLRUCacheTest.VolatileLRUCacheSet("vivek", "vivek", binary.Size([]byte("vivek")), time.Duration(0))
	size = size + binary.Size([]byte("ibibo"))
	VolatileLRUCacheTest.VolatileLRUCacheSet("ibibo", "ibibo", binary.Size([]byte("ibibo")), time.Duration(0))
	size = size + binary.Size([]byte("spectre"))
	VolatileLRUCacheTest.VolatileLRUCacheSet("spectre", "spectre", binary.Size([]byte("spectre")), time.Duration(0))
	time.Sleep(1000000000)
	returnedSize := VolatileLRUCacheTest.VolatileLRUCacheCurrentSize()

	if returnedSize != size {
		t.Fatalf("cache size calculation is incorrect")
	}
}

func TestVolatileLRUCacheIterator(t *testing.T) {
	VolatileLRUCacheTest.VolatileLRUCacheSet("vivek", "vivek", binary.Size([]byte("vivek")), time.Duration(0))
	VolatileLRUCacheTest.VolatileLRUCacheSet("ibibo", "ibibo", binary.Size([]byte("ibibo")), time.Duration(0))
	VolatileLRUCacheTest.VolatileLRUCacheSet("spectre", "spectre", binary.Size([]byte("spectre")), time.Duration(0))
	expectedOutput := make(map[string]CacheRow)
	expectedOutput["vivek"] = CacheRow{Key: "vivek", Value: "vivek"}
	expectedOutput["ibibo"] = CacheRow{Key: "ibibo", Value: "ibibo"}
	expectedOutput["spectre"] = CacheRow{Key: "spectre", Value: "spectre"}
	outputChannel := make(chan CacheRow)
	VolatileLRUCacheTest.VolatileLRUCacheIterator(outputChannel)
	for data := range outputChannel {
		if data != expectedOutput[data.Key] {
			t.Fatalf("iterator fails")
		}
	}
}

func BenchmarkVolatileLRUCacheIterator(b *testing.B) {
	VolatileLRUCacheTest.VolatileLRUCacheSet("vivek", "vivek", binary.Size([]byte("vivek")), time.Duration(0))
	VolatileLRUCacheTest.VolatileLRUCacheSet("ibibo", "ibibo", binary.Size([]byte("ibibo")), time.Duration(0))
	VolatileLRUCacheTest.VolatileLRUCacheSet("spectre", "spectre", binary.Size([]byte("spectre")), time.Duration(0))
	expectedOutput := make(map[string]CacheRow)
	expectedOutput["vivek"] = CacheRow{Key: "vivek", Value: "vivek"}
	expectedOutput["ibibo"] = CacheRow{Key: "ibibo", Value: "ibibo"}
	expectedOutput["spectre"] = CacheRow{Key: "spectre", Value: "spectre"}
	for n := 0; n < b.N; n++ {
		outputChannel := make(chan CacheRow)
		VolatileLRUCacheTest.VolatileLRUCacheIterator(outputChannel)
		for data := range outputChannel {
			if data != expectedOutput[data.Key] {
				b.Fatalf("iterator fails")
			}
		}
	}
}

func TestVolatileLRUCacheGet(t *testing.T) {
	VolatileLRUCacheTest.VolatileLRUCacheSet("vivek", "vivek", binary.Size([]byte("vivek")), time.Duration(0))
	VolatileLRUCacheTest.VolatileLRUCacheSet("ibibo", "ibibo", binary.Size([]byte("ibibo")), time.Duration(0))
	VolatileLRUCacheTest.VolatileLRUCacheSet("spectre", "spectre", binary.Size([]byte("spectre")), time.Duration(0))
	expectedOutput := make(map[string]string)
	expectedOutput["vivek"] = "vivek"
	expectedOutput["ibibo"] = "ibibo"
	expectedOutput["spectre"] = "spectre"
	for key, data := range expectedOutput {
		val, _ := VolatileLRUCacheTest.VolatileLRUCacheGet(key)
		if data != val {
			t.Fatalf("get unit test case fails")
		}
	}
}

func BenchmarkVolatileLRUCacheGet(b *testing.B) {
	VolatileLRUCacheTest.VolatileLRUCacheSet("vivek", "vivek", binary.Size([]byte("vivek")), time.Duration(0))
	VolatileLRUCacheTest.VolatileLRUCacheSet("ibibo", "ibibo", binary.Size([]byte("ibibo")), time.Duration(0))
	VolatileLRUCacheTest.VolatileLRUCacheSet("spectre", "spectre", binary.Size([]byte("spectre")), time.Duration(0))
	expectedOutput := make(map[string]string)
	expectedOutput["vivek"] = "vivek"
	expectedOutput["ibibo"] = "ibibo"
	expectedOutput["spectre"] = "spectre"

	for n := 0; n < b.N; n++ {
		for key, data := range expectedOutput {
			val, _ := VolatileLRUCacheTest.VolatileLRUCacheGet(key)
			if data != val {
				b.Fatalf("get unit test case fails")
			}
		}
	}
}

func TestVolatileLRUCacheSet(t *testing.T) {
	success, error := VolatileLRUCacheTest.VolatileLRUCacheSet("vivek", "vivek", binary.Size([]byte("vivek")), time.Duration(0))
	if !success {
		t.Fatalf("data setting got failed in Cache with algo %v", error)
	}
}

func BenchmarkVolatileLRUCacheSet(b *testing.B) {
	// run the Cache set  function b.N times
	for n := 0; n < b.N; n++ {
		VolatileLRUCacheTest.VolatileLRUCacheSet("vivek", "vivek", binary.Size([]byte("vivek")), time.Duration(0))
	}
}

func TestVolatileLRUCacheDelete(t *testing.T) {
	success, error := VolatileLRUCacheTest.VolatileLRUCacheSet("vivek", "vivek", binary.Size([]byte("vivek")), time.Duration(0))
	if !success {
		t.Fatalf("could not set data to test delete")
	}
	VolatileLRUCacheTest.VolatileLRUCacheDelete("vivek")
	_, success = VolatileLRUCacheTest.VolatileLRUCacheGet("vivek")
	if success {
		t.Fatalf("could not delete the element in Cache. Cache delete got failed", error)
	}
}

func BenchmarkVolatileLRUCacheDelete(b *testing.B) {
	// run the Cache set  function b.N times
	for n := 0; n < b.N; n++ {
		VolatileLRUCacheTest.VolatileLRUCacheClear()
		success, _ := VolatileLRUCacheTest.VolatileLRUCacheSet("vivek", "vivek", binary.Size([]byte("vivek")), time.Duration(0))
		if !success {
			b.Fatalf("could not set data to test delete")
		}
		VolatileLRUCacheTest.VolatileLRUCacheDelete("vivek")
		_, success = VolatileLRUCacheTest.VolatileLRUCacheGet("vivek")
		if success {
			b.Fatalf("could not delete the element in Cache. Cache delete got failed")
		}
	}
}

func TestVolatileLRUCacheClearCache(t *testing.T) {
	success, _ := VolatileLRUCacheTest.VolatileLRUCacheSet("vivek", "vivek", binary.Size([]byte("vivek")), time.Duration(0))
	if !success {
		t.Fatalf("TestClearCache could not set data to test delete")
	}
	VolatileLRUCacheTest.VolatileLRUCacheClear()
	_, success = VolatileLRUCacheTest.VolatileLRUCacheGet("vivek")
	if success {
		t.Fatalf("CacheClear unit test failed")
	}
}

func BenchmarkVolatileLRUCacheClearCache(b *testing.B) {
	// run the Cache set  function b.N times
	for n := 0; n < b.N; n++ {
		success, _ := VolatileLRUCacheTest.VolatileLRUCacheSet("vivek", "vivek", binary.Size([]byte("vivek")), time.Duration(0))
		if !success {
			b.Fatalf("BenchmarkClearCache could not set data to test delete")
		}
		VolatileLRUCacheTest.VolatileLRUCacheDelete("vivek")
		_, success = VolatileLRUCacheTest.VolatileLRUCacheGet("vivek")
		if success {
			b.Fatalf("CacheClear benchmark test failed")
		}
	}
}

func TestGetVolatileLRUCache(t *testing.T) {
	if reflect.ValueOf(VolatileLRUCacheTest).Type().String() != "*spectre.VolatileLRUCache" {
		t.Fatalf("type of the cache initiated is not spectre.Cache")
	}
}
