/*
Package timedb implements a minimal time series database.

It is fast for sequential reads and writes. Its most common use case is storing
server monitoring logs or any other kind of log.
*/
package timedb

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DB struct {
	Path      string
	mutex     *sync.RWMutex
	file      *os.File
	writePath string
}

func New(path string) *DB {
	return &DB{Path: path, mutex: &sync.RWMutex{}}
}

func (db *DB) Save(table, data string, v ...interface{}) error {
	return db.save(time.Now(), table, data, v...)
}

func (db *DB) Insert(t time.Time, table, data string, v ...interface{}) error {
	// todo: hacer que inserte de verdad
	return db.save(t, table, data, v...)
}

type DataPoint struct {
	Time time.Time
	Text string
}

func (d DataPoint) String() string {
	return d.Time.Format("2006-01-02 15:04:05") + " " + d.Text
}

type Scanner struct {
	reader  *reader
	scanner *bufio.Scanner
	Error   error
}

func (s *Scanner) Scan() bool {
	r := s.reader
	sc := s.scanner

LOOP:
	for {
		if r.limit > 0 && r.index >= r.limit {
			return false
		}

		if ok := sc.Scan(); !ok {
			return false
		}

		d := s.Data()
		// advance to start before sending data
		if d.Time.Before(r.start) {
			continue LOOP
		}

		if r.filter != "" {
			if !strings.Contains(sc.Text(), r.filter) {
				continue LOOP
			}
		}

		// advance to Offset before sending data
		for r.index < r.offset {
			r.index++
			continue LOOP
		}

		r.index++
		return true
	}
}

func (s *Scanner) Close() {
	s.reader.Close()
}

func (s *Scanner) SetFilter(v string) {
	s.reader.filter = v
}

func (s *Scanner) Data() DataPoint {
	line := s.scanner.Text()

	err := s.scanner.Err()
	if err != nil {
		s.Error = err
		return DataPoint{}
	}

	i := strings.Index(line, " ")
	if i == -1 {
		s.Error = fmt.Errorf("Invalid line: %s", line)
		return DataPoint{}
	}

	epoch, err := strconv.ParseInt(line[:i], 10, 64)
	if err != nil {
		s.Error = fmt.Errorf("Error parsing time in '%s': %v", line, err)
		return DataPoint{}
	}

	return DataPoint{Time: time.Unix(int64(epoch), 0), Text: line[i:]}
}

func (db *DB) Query(table string, start, end time.Time, offset, size int) *Scanner {
	r := db.reader(start, end, table, offset, offset+size)
	s := bufio.NewScanner(r)

	// set large capacity (some lines ar very long)
	const maxCapacity = 512 * 1024
	buf := make([]byte, maxCapacity)
	s.Buffer(buf, maxCapacity)

	return &Scanner{
		scanner: s,
		reader:  r,
	}
}

type reader struct {
	db       *DB
	table    string
	start    time.Time
	end      time.Time
	offset   int
	limit    int
	index    int
	filter   string
	current  time.Time
	file     *os.File
	keepFile bool
	buf      []byte
}

// Read reads up to len(p) bytes through one or many files
func (r *reader) Read(p []byte) (int, error) {
	r.db.mutex.RLock()
	defer r.db.mutex.RUnlock()

	l := len(p)

	for {
		// if there is enough data, send it
		if len(r.buf) >= l {
			copy(p, r.buf[:l])
			r.buf = r.buf[l:]
			return l, nil
		}

		// advance to the next file in necessary
		if !r.keepFile {
			err := r.nextFile()
			if err != nil {
				var n int
				if err == io.EOF {
					// Last file reached. Copy the remaining data
					if len(r.buf) > 0 {
						n = copy(p, r.buf)
					}
				}
				return n, err
			}
		}

		// read the current file and grow the buffer
		b := make([]byte, len(p))
		n, err := r.file.Read(b)
		if err == io.EOF {
			r.keepFile = false
		} else if err != nil {
			return n, err
		} else {
			r.keepFile = true
		}

		r.buf = append(r.buf, b[:n]...)
	}
}

func (r *reader) nextFile() error {
	for {
		// poner al inicio o avanzar un día
		if r.current.Before(r.start) {
			r.current = r.start
		} else {
			r.current = r.current.Add(time.Hour * 24)
		}

		// controlar si nos  hemos pasado de fecha
		if r.current.After(r.end) {
			return io.EOF
		}

		file, err := r.open(r.current)
		if err != nil {
			// si este día no hay datos pasar al siguiente
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		r.file = file
		return nil
	}
}

func (r *reader) Close() {
	if r.file != nil {
		r.file.Close()
		r.file = nil
	}
}

func (r *reader) open(t time.Time) (*os.File, error) {
	// Close the previous one if exists
	r.Close()

	path := r.db.getTablePath(t, r.table)

	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
		return nil, fmt.Errorf("timeDB.open: error openning file %s: %v", path, err)
	}
	return f, nil
}

func (db *DB) reader(start, end time.Time, table string, offset, limit int) *reader {
	return &reader{
		db:     db,
		start:  start.Local(),
		end:    end.Local(),
		table:  table,
		offset: offset,
		limit:  limit,
	}
}

func (db *DB) getDir(t time.Time) string {
	return filepath.Join(db.Path, t.Format("2006-01-02"))
}

func (db *DB) getTablePath(t time.Time, table string) string {
	return filepath.Join(db.getDir(t), table+".log")
}

func (db *DB) save(t time.Time, table, data string, v ...interface{}) error {
	if len(v) > 0 {
		data = fmt.Sprintf(data, v...)
	}

	dirName := db.getDir(t)
	fileName := db.getTablePath(t, table)

	db.mutex.Lock()

	if db.file == nil || db.writePath != fileName {
		if db.file != nil {
			db.file.Close()
			db.file = nil
		}

		err := os.MkdirAll(dirName, 0777)
		if err != nil {
			return fmt.Errorf("timeDB: error creating dir %s: %v", dirName, err)
		}

		f, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("timeDB.Save: error openning file %s: %v", fileName, err)
		}

		db.file = f
		db.writePath = fileName
	}

	if _, err := fmt.Fprintf(db.file, "%d %s\n", t.Unix(), data); err != nil {
		return fmt.Errorf("timeDB: error writing data %v", err)
	}

	db.mutex.Unlock()
	return nil
}
