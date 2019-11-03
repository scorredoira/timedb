package timedb

import (
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func BenchmarkWrite(b *testing.B) {
	os.RemoveAll("data")
	db := New("data")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		write(db, b)
	}

	os.RemoveAll("data")
}

func BenchmarkWriteSqlite(b *testing.B) {
	db := openSqlite(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		writeSqlite(db, b)
	}

	os.RemoveAll("test.db")
}

func BenchmarkRead(b *testing.B) {
	os.RemoveAll("data")
	db := New("data")
	write(db, b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		scanner := db.Query("logs", start, start, 0, 0)
		for scanner.Scan() {
			_ = scanner.Data()
			if scanner.Error != nil {
				b.Fatal(scanner.Error)
			}
		}
		scanner.Close()
	}

	os.RemoveAll("data")
}

func BenchmarkReadSqlite(b *testing.B) {
	db := openSqlite(b)
	writeSqlite(db, b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		start := time.Now().Add(time.Minute * -10)
		end := time.Now()
		rows, err := db.Query("SELECT d, data FROM logs WHERE d >=? and d <=?", start, end)
		if err != nil {
			b.Fatal(err)
		}
		var t time.Time
		var data string
		for rows.Next() {
			err = rows.Scan(&t, &data)
			if err != nil {
				b.Fatal(err)
			}
		}

		rows.Close()
	}

	os.RemoveAll("test.db")
}

func write(db *DB, b *testing.B) {
	t := time.Now()
	for i := 0; i < 1000; i++ {
		err := db.Insert(t, "logs", "this is a test.")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func writeSqlite(db *sql.DB, b *testing.B) {
	t := time.Now()
	for i := 0; i < 1000; i++ {
		// We could do all inserts in a transaction and it would be much faster
		// but we are also openning and closing the file on each write in the
		// other write test because thats the expected use case.
		_, err := db.Exec("INSERT INTO logs VALUES (?,?)", t, "this is a test.")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func openSqlite(b *testing.B) *sql.DB {
	os.RemoveAll("test.db")
	db, err := sql.Open("sqlite3", "test.db")
	if err != nil {
		b.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE logs (d DATETIME NOT NULL, data TEXT NOT NULL);`)
	if err != nil {
		b.Fatal(err)
	}

	return db
}
