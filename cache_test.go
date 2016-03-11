package spectre

import (
	"encoding/binary"
	"reflect"
	"testing"
)

var testCache = GetDefaultCache(5000, 15)

func TestGetCurrentSize(t *testing.T) {
	var size = 0
	size = size + binary.Size([]byte("vivek"))
	testCache.CacheSet("vivek", "vivek", binary.Size([]byte("vivek")))
	size = size + binary.Size([]byte("ibibo"))
	testCache.CacheSet("ibibo", "ibibo", binary.Size([]byte("ibibo")))
	size = size + binary.Size([]byte("spectre"))
	testCache.CacheSet("spectre", "spectre", binary.Size([]byte("spectre")))
	returnedSize := testCache.GetCurrentSize()
	if returnedSize != size {
		t.Fatalf("cache size calculation is incorrect")
	}
}

func TestCacheIterator(t *testing.T) {
	testCache.CacheSet("vivek", "vivek", binary.Size([]byte("vivek")))
	testCache.CacheSet("ibibo", "ibibo", binary.Size([]byte("ibibo")))
	testCache.CacheSet("spectre", "spectre", binary.Size([]byte("spectre")))
	expectedOutput := make(map[string]CacheRow)
	expectedOutput["vivek"] = CacheRow{Key: "vivek", Value: "vivek"}
	expectedOutput["ibibo"] = CacheRow{Key: "ibibo", Value: "ibibo"}
	expectedOutput["spectre"] = CacheRow{Key: "spectre", Value: "spectre"}
	outputChannel := make(chan CacheRow)
	testCache.CacheIterator(outputChannel)
	for data := range outputChannel {
		if data != expectedOutput[data.Key] {
			t.Fatalf("iterator fails")
		}
	}
}

func BenchmarkCacheIterator(b *testing.B) {
	testCache.CacheSet("vivek", "vivek", binary.Size([]byte("vivek")))
	testCache.CacheSet("ibibo", "ibibo", binary.Size([]byte("ibibo")))
	testCache.CacheSet("spectre", "spectre", binary.Size([]byte("spectre")))
	expectedOutput := make(map[string]CacheRow)
	expectedOutput["vivek"] = CacheRow{Key: "vivek", Value: "vivek"}
	expectedOutput["ibibo"] = CacheRow{Key: "ibibo", Value: "ibibo"}
	expectedOutput["spectre"] = CacheRow{Key: "spectre", Value: "spectre"}
	for n := 0; n < b.N; n++ {
		outputChannel := make(chan CacheRow)
		testCache.CacheIterator(outputChannel)
		for data := range outputChannel {
			if data != expectedOutput[data.Key] {
				b.Fatalf("iterator fails")
			}
		}
	}
}

func TestCacheGet(t *testing.T) {
	testCache.CacheSet("vivek", "vivek", binary.Size([]byte("vivek")))
	testCache.CacheSet("ibibo", "ibibo", binary.Size([]byte("ibibo")))
	testCache.CacheSet("spectre", "spectre", binary.Size([]byte("spectre")))
	expectedOutput := make(map[string]string)
	expectedOutput["vivek"] = "vivek"
	expectedOutput["ibibo"] = "ibibo"
	expectedOutput["spectre"] = "spectre"
	for key, data := range expectedOutput {
		val, _ := testCache.CacheGet(key)
		if data != val {
			t.Fatalf("get unit test case fails")
		}
	}
}

func BenchmarkCacheGet(b *testing.B) {
	testCache.CacheSet("vivek", "vivek", binary.Size([]byte("vivek")))
	testCache.CacheSet("ibibo", "ibibo", binary.Size([]byte("ibibo")))
	testCache.CacheSet("spectre", "spectre", binary.Size([]byte("spectre")))
	expectedOutput := make(map[string]string)
	expectedOutput["vivek"] = "vivek"
	expectedOutput["ibibo"] = "ibibo"
	expectedOutput["spectre"] = "spectre"
	for n := 0; n < b.N; n++ {
		for key, data := range expectedOutput {
			val, _ := testCache.CacheGet(key)
			if data != val {
				b.Fatalf("get unit test case fails")
			}
		}
	}
}

func TestCacheSet(t *testing.T) {
	success, error := testCache.CacheSet("vivek", "vivek", binary.Size([]byte("vivek")))
	if !success {
		t.Fatalf("data setting got failed in Cache with algo %v", error)
	}
}

func BenchmarkCacheSet(b *testing.B) {
	// run the Cache set  function b.N times
	for n := 0; n < b.N; n++ {
		testCache.CacheSet("vivek", "vivek", binary.Size([]byte("vivek")))
	}
}

func TestSetData(t *testing.T) {
	success, error := testCache.SetData("vivek", "vivek", binary.Size([]byte("vivek")))
	if !success {
		t.Fatalf("data setting got failed in Cache without algo %v", error)
	}
}

func BenchmarkSetData(b *testing.B) {
	// run the Cache set  function b.N times
	for n := 0; n < b.N; n++ {
		testCache.CacheSet("vivek", "vivek", binary.Size([]byte("vivek")))
	}
}

func TestCacheDelete(t *testing.T) {
	success, error := testCache.CacheSet("vivek", "vivek", binary.Size([]byte("vivek")))
	if !success {
		t.Fatalf("could not set data to test delete")
	}
	testCache.CacheDelete("vivek")
	_, success = testCache.CacheGet("vivek")
	if success {
		t.Fatalf("could not delete the element in Cache. Cache delete got failed", error)
	}
}

func BenchmarkCacheDelete(b *testing.B) {
	// run the Cache set  function b.N times
	for n := 0; n < b.N; n++ {
		testCache.ClearCache()
		success, _ := testCache.CacheSet("vivek", "vivek", binary.Size([]byte("vivek")))
		if !success {
			b.Fatalf("could not set data to test delete")
		}
		testCache.CacheDelete("vivek")
		_, success = testCache.CacheGet("vivek")
		if success {
			b.Fatalf("could not delete the element in Cache. Cache delete got failed")
		}
	}
}

func TestClearCache(t *testing.T) {
	success, _ := testCache.CacheSet("vivek", "vivek", binary.Size([]byte("vivek")))
	if !success {
		t.Fatalf("TestClearCache could not set data to test delete")
	}
	testCache.ClearCache()
	_, success = testCache.CacheGet("vivek")
	if success {
		t.Fatalf("CacheClear unit test failed")
	}
}

func BenchmarkClearCache(b *testing.B) {
	// run the Cache set  function b.N times
	for n := 0; n < b.N; n++ {
		success, _ := testCache.CacheSet("vivek", "vivek", binary.Size([]byte("vivek")))
		if !success {
			b.Fatalf("BenchmarkClearCache could not set data to test delete")
		}
		testCache.CacheDelete("vivek")
		_, success = testCache.CacheGet("vivek")
		if success {
			b.Fatalf("CacheClear benchmark test failed")
		}
	}
}

func TestGetDefaultCache(t *testing.T) {
	if reflect.ValueOf(testCache).Type().String() != "*spectre.Cache" {
		t.Fatalf("type of the cache initiated is not spectre.Cache")
	}
}
