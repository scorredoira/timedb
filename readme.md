## A minimal time series database.

It is fast for sequential reads and writes. Its most common use case is storing
server monitoring logs or any other kind of log.

## How to use it

Write:

```go
db := New("path/to/data")

err := db.Insert(time.Now(), "log", "something")
```
	
Read:

```go
db := New("path/to/data")

scanner := db.Query("logs", time.Now(), time.Now())	
for scanner.Scan() {
	datapoint := scanner.Data()
}	
```
	

	$ go test -test.bench=.* --benchmem
	goos: linux
	goarch: amd64
	pkg: scorredoira/timedb
	BenchmarkWrite-12                    169           6741528 ns/op          128372 B/op       8000 allocs/op
	BenchmarkWriteSqlite-12                1        1351823225 ns/op          480016 B/op      16001 allocs/op
	BenchmarkRead-12                    3957            472869 ns/op         1632696 B/op       1015 allocs/op
	BenchmarkReadSqlite-12               726           1618538 ns/op          257056 B/op       7031 allocs/op