package kvbench

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"

	minweight "github.com/JimChengLin/minweight_store"
	"github.com/cockroachdb/pebble"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/syndtr/goleveldb/leveldb"
	leveldberrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tidwall/buntdb"
	bbolt "go.etcd.io/bbolt"
)

const (
	benchDatasetSize = 100_000
	benchValueSize   = 256
	bboltBucketName  = "kv"
)

var errKeyNotFound = errors.New("key not found")

type pointStore interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	Delete(key []byte) error
	Close() error
}

type orderedStore interface {
	pointStore
	Scan(fn func(key, value []byte) bool) error
	SeekGE(key []byte) ([]byte, []byte, bool, error)
}

type storeFactory struct {
	name string
	open func(*testing.B, string) (pointStore, error)
}

func storeFactories() []storeFactory {
	return []storeFactory{
		{name: "minweight", open: openMinweightStore},
		{name: "badger", open: openBadgerStore},
		{name: "bbolt", open: openBboltStore},
		{name: "buntdb", open: openBuntDBStore},
		{name: "goleveldb", open: openLevelDBStore},
		{name: "pebble", open: openPebbleStore},
		{name: "map", open: openMapStore},
	}
}

func BenchmarkSet(b *testing.B) {
	keys := benchKeys(benchDatasetSize)
	values := benchValues(benchDatasetSize)
	for _, factory := range storeFactories() {
		b.Run(factory.name, func(b *testing.B) {
			store := openBenchStore(b, factory)
			b.ReportAllocs()
			b.SetBytes(benchValueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx := i % benchDatasetSize
				if err := store.Put(keys[idx], values[idx]); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			closeBenchStore(b, store)
		})
	}
}

func BenchmarkGet(b *testing.B) {
	keys := benchKeys(benchDatasetSize)
	values := benchValues(benchDatasetSize)
	for _, factory := range storeFactories() {
		b.Run(factory.name, func(b *testing.B) {
			store := openBenchStore(b, factory)
			preloadBenchStore(b, store, keys, values)
			b.ReportAllocs()
			b.SetBytes(benchValueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				value, ok, err := store.Get(keys[i%benchDatasetSize])
				if err != nil || !ok {
					b.Fatalf("Get = (%d,%v,%v), want value,true,nil", len(value), ok, err)
				}
			}
			b.StopTimer()
			closeBenchStore(b, store)
		})
	}
}

func BenchmarkDelete(b *testing.B) {
	keys := benchKeys(benchDatasetSize)
	values := benchValues(benchDatasetSize)
	for _, factory := range storeFactories() {
		b.Run(factory.name, func(b *testing.B) {
			store := openBenchStore(b, factory)
			done := 0
			b.ReportAllocs()
			b.SetBytes(benchValueSize)
			for done < b.N {
				n := min(benchDatasetSize, b.N-done)
				b.StopTimer()
				preloadBenchStore(b, store, keys[:n], values[:n])
				b.StartTimer()
				for i := 0; i < n; i++ {
					if err := store.Delete(keys[i]); err != nil {
						b.Fatal(err)
					}
				}
				done += n
			}
			b.StopTimer()
			closeBenchStore(b, store)
		})
	}
}

func BenchmarkMixedReadWrite(b *testing.B) {
	keys := benchKeys(benchDatasetSize)
	values := benchValues(benchDatasetSize)
	for _, factory := range storeFactories() {
		b.Run(factory.name, func(b *testing.B) {
			store := openBenchStore(b, factory)
			preloadBenchStore(b, store, keys, values)
			b.ReportAllocs()
			b.SetBytes(benchValueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx := i % benchDatasetSize
				if i%10 == 0 {
					if err := store.Put(keys[idx], values[(idx+1)%benchDatasetSize]); err != nil {
						b.Fatal(err)
					}
					continue
				}
				if _, ok, err := store.Get(keys[idx]); err != nil || !ok {
					b.Fatalf("Get = (%v,%v), want true,nil", ok, err)
				}
			}
			b.StopTimer()
			closeBenchStore(b, store)
		})
	}
}

func BenchmarkScan(b *testing.B) {
	keys := benchKeys(benchDatasetSize)
	values := benchValues(benchDatasetSize)
	for _, factory := range storeFactories() {
		b.Run(factory.name, func(b *testing.B) {
			store := openBenchStore(b, factory)
			ordered, ok := store.(orderedStore)
			if !ok {
				b.Skip("store has no ordered scan")
			}
			preloadBenchStore(b, ordered, keys, values)
			b.ReportAllocs()
			b.SetBytes(int64(benchDatasetSize * benchValueSize))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := 0
				if err := ordered.Scan(func(_, _ []byte) bool {
					count++
					return true
				}); err != nil {
					b.Fatal(err)
				}
				if count != benchDatasetSize {
					b.Fatalf("scan count = %d, want %d", count, benchDatasetSize)
				}
			}
			b.ReportMetric(float64(b.N*benchDatasetSize)/b.Elapsed().Seconds(), "entries/s")
			b.StopTimer()
			closeBenchStore(b, ordered)
		})
	}
}

func BenchmarkSeekGE(b *testing.B) {
	keys := benchKeys(benchDatasetSize)
	values := benchValues(benchDatasetSize)
	for _, factory := range storeFactories() {
		b.Run(factory.name, func(b *testing.B) {
			store := openBenchStore(b, factory)
			ordered, ok := store.(orderedStore)
			if !ok {
				b.Skip("store has no ordered seek")
			}
			preloadBenchStore(b, ordered, keys, values)
			b.ReportAllocs()
			b.SetBytes(benchValueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, ok, err := ordered.SeekGE(keys[i%benchDatasetSize])
				if err != nil || !ok {
					b.Fatalf("SeekGE = (%v,%v), want true,nil", ok, err)
				}
			}
			b.StopTimer()
			closeBenchStore(b, ordered)
		})
	}
}

func BenchmarkMinweightTuningSet(b *testing.B) {
	keys := benchKeys(benchDatasetSize)
	values := benchValues(benchDatasetSize)
	for _, variant := range minweightTuningVariants() {
		b.Run(variant.name, func(b *testing.B) {
			store := openMinweightTuningStore(b, variant.options)
			b.ReportAllocs()
			b.SetBytes(benchValueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx := i % benchDatasetSize
				if err := store.Put(keys[idx], values[idx]); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			closeBenchStore(b, store)
		})
	}
}

func BenchmarkMinweightTuningMixedReadWrite(b *testing.B) {
	keys := benchKeys(benchDatasetSize)
	values := benchValues(benchDatasetSize)
	for _, variant := range minweightTuningVariants() {
		b.Run(variant.name, func(b *testing.B) {
			store := openMinweightTuningStore(b, variant.options)
			preloadBenchStore(b, store, keys, values)
			b.ReportAllocs()
			b.SetBytes(benchValueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx := i % benchDatasetSize
				if i%10 == 0 {
					if err := store.Put(keys[idx], values[(idx+1)%benchDatasetSize]); err != nil {
						b.Fatal(err)
					}
					continue
				}
				if _, ok, err := store.Get(keys[idx]); err != nil || !ok {
					b.Fatalf("Get = (%v,%v), want true,nil", ok, err)
				}
			}
			b.StopTimer()
			closeBenchStore(b, store)
		})
	}
}

func openBenchStore(b *testing.B, factory storeFactory) pointStore {
	b.Helper()

	dir := benchStoreDir(b, factory.name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatal(err)
	}
	store, err := factory.open(b, dir)
	if err != nil {
		b.Fatal(err)
	}
	return store
}

func closeBenchStore(b *testing.B, store pointStore) {
	b.Helper()

	if err := store.Close(); err != nil {
		b.Fatal(err)
	}
}

func preloadBenchStore(b *testing.B, store pointStore, keys, values [][]byte) {
	b.Helper()

	for i := range keys {
		if err := store.Put(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func benchKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%06d", i))
	}
	return keys
}

func benchValues(n int) [][]byte {
	values := make([][]byte, n)
	for i := range values {
		value := bytes.Repeat([]byte{byte(i)}, benchValueSize)
		values[i] = value
	}
	return values
}

func cloneBytes(v []byte) []byte {
	return append([]byte(nil), v...)
}

type minweightStore struct {
	store *minweight.Store
}

func openMinweightStore(_ *testing.B, dir string) (pointStore, error) {
	return openMinweightStoreWithOptions(dir, minweight.Options{})
}

type minweightTuningVariant struct {
	name    string
	options minweight.Options
}

func minweightTuningVariants() []minweightTuningVariant {
	return []minweightTuningVariant{
		{name: "default"},
		{name: "wal512m", options: minweight.Options{WALSize: 512 << 20}},
		{name: "wal1g", options: minweight.Options{WALSize: 1 << 30}},
		{name: "keep_wal", options: minweight.Options{MaxImmutableWALNum: 1000}},
		{name: "wal1g_keep_wal", options: minweight.Options{WALSize: 1 << 30, MaxImmutableWALNum: 1000}},
	}
}

func openMinweightTuningStore(b *testing.B, options minweight.Options) pointStore {
	b.Helper()

	store, err := openMinweightStoreWithOptions(benchStoreDir(b, "minweight"), options)
	if err != nil {
		b.Fatal(err)
	}
	return store
}

func openMinweightStoreWithOptions(dir string, options minweight.Options) (pointStore, error) {
	options.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := minweight.Open(dir, options)
	if err != nil {
		return nil, err
	}
	return &minweightStore{store: store}, nil
}

func benchStoreDir(b *testing.B, name string) string {
	b.Helper()

	root := os.Getenv("KVBENCH_DATA_DIR")
	if root == "" {
		return filepath.Join(b.TempDir(), name)
	}
	dir, err := os.MkdirTemp(root, sanitizeBenchName(b.Name()+"-"+name)+"-")
	if err != nil {
		b.Fatal(err)
	}
	if os.Getenv("KVBENCH_KEEP_DATA") == "" {
		b.Cleanup(func() {
			_ = os.RemoveAll(dir)
		})
	}
	return dir
}

func sanitizeBenchName(name string) string {
	buf := make([]byte, 0, len(name))
	for _, r := range name {
		if r >= 'a' && r <= 'z' ||
			r >= 'A' && r <= 'Z' ||
			r >= '0' && r <= '9' ||
			r == '-' ||
			r == '_' ||
			r == '.' {
			buf = append(buf, byte(r))
			continue
		}
		buf = append(buf, '_')
	}
	return string(buf)
}

func (s *minweightStore) Put(key, value []byte) error {
	return s.store.Put(key, value)
}

func (s *minweightStore) Get(key []byte) ([]byte, bool, error) {
	return s.store.Get(key)
}

func (s *minweightStore) Delete(key []byte) error {
	_, err := s.store.Delete(key)
	return err
}

func (s *minweightStore) Scan(fn func(key, value []byte) bool) error {
	return s.store.Scan(func(item minweight.Item) bool {
		return fn(item.Key, item.Value)
	})
}

func (s *minweightStore) SeekGE(key []byte) ([]byte, []byte, bool, error) {
	item, ok, err := s.store.SeekGE(key)
	return item.Key, item.Value, ok, err
}

func (s *minweightStore) Close() error {
	return s.store.Close()
}

type badgerStore struct {
	db *badger.DB
}

func openBadgerStore(_ *testing.B, dir string) (pointStore, error) {
	opts := badger.DefaultOptions(dir).WithLogger(nil).WithSyncWrites(false)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &badgerStore{db: db}, nil
}

func (s *badgerStore) Put(key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (s *badgerStore) Get(key []byte) ([]byte, bool, error) {
	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return errKeyNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			value = cloneBytes(v)
			return nil
		})
	})
	if errors.Is(err, errKeyNotFound) {
		return nil, false, nil
	}
	return value, err == nil, err
}

func (s *badgerStore) Delete(key []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (s *badgerStore) Scan(fn func(key, value []byte) bool) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(value []byte) error {
				if !fn(item.Key(), value) {
					return errKeyNotFound
				}
				return nil
			})
			if errors.Is(err, errKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *badgerStore) SeekGE(key []byte) ([]byte, []byte, bool, error) {
	var gotKey []byte
	var gotValue []byte
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(key)
		if !it.Valid() {
			return errKeyNotFound
		}
		item := it.Item()
		gotKey = cloneBytes(item.Key())
		return item.Value(func(value []byte) error {
			gotValue = cloneBytes(value)
			return nil
		})
	})
	if errors.Is(err, errKeyNotFound) {
		return nil, nil, false, nil
	}
	return gotKey, gotValue, err == nil, err
}

func (s *badgerStore) Close() error {
	return s.db.Close()
}

type bboltStore struct {
	db *bbolt.DB
}

func openBboltStore(_ *testing.B, dir string) (pointStore, error) {
	db, err := bbolt.Open(filepath.Join(dir, "db.bbolt"), 0o600, &bbolt.Options{NoSync: true})
	if err != nil {
		return nil, err
	}
	store := &bboltStore{db: db}
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bboltBucketName))
		return err
	})
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func (s *bboltStore) Put(key, value []byte) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte(bboltBucketName)).Put(key, value)
	})
}

func (s *bboltStore) Get(key []byte) ([]byte, bool, error) {
	var value []byte
	err := s.db.View(func(tx *bbolt.Tx) error {
		got := tx.Bucket([]byte(bboltBucketName)).Get(key)
		if got == nil {
			return nil
		}
		value = cloneBytes(got)
		return nil
	})
	return value, value != nil, err
}

func (s *bboltStore) Delete(key []byte) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte(bboltBucketName)).Delete(key)
	})
}

func (s *bboltStore) Scan(fn func(key, value []byte) bool) error {
	return s.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket([]byte(bboltBucketName)).Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			if !fn(key, value) {
				break
			}
		}
		return nil
	})
}

func (s *bboltStore) SeekGE(key []byte) ([]byte, []byte, bool, error) {
	var gotKey []byte
	var gotValue []byte
	err := s.db.View(func(tx *bbolt.Tx) error {
		key, value := tx.Bucket([]byte(bboltBucketName)).Cursor().Seek(key)
		if key == nil {
			return nil
		}
		gotKey = cloneBytes(key)
		gotValue = cloneBytes(value)
		return nil
	})
	return gotKey, gotValue, gotKey != nil, err
}

func (s *bboltStore) Close() error {
	return s.db.Close()
}

type buntDBStore struct {
	db *buntdb.DB
}

func openBuntDBStore(_ *testing.B, dir string) (pointStore, error) {
	db, err := buntdb.Open(filepath.Join(dir, "db.buntdb"))
	if err != nil {
		return nil, err
	}
	store := &buntDBStore{db: db}
	if err := db.SetConfig(buntdb.Config{SyncPolicy: buntdb.Never}); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func (s *buntDBStore) Put(key, value []byte) error {
	return s.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(string(key), string(value), nil)
		return err
	})
}

func (s *buntDBStore) Get(key []byte) ([]byte, bool, error) {
	var value []byte
	err := s.db.View(func(tx *buntdb.Tx) error {
		got, err := tx.Get(string(key))
		if errors.Is(err, buntdb.ErrNotFound) {
			return errKeyNotFound
		}
		if err != nil {
			return err
		}
		value = []byte(got)
		return nil
	})
	if errors.Is(err, errKeyNotFound) {
		return nil, false, nil
	}
	return value, err == nil, err
}

func (s *buntDBStore) Delete(key []byte) error {
	return s.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(string(key))
		if errors.Is(err, buntdb.ErrNotFound) {
			return nil
		}
		return err
	})
}

func (s *buntDBStore) Scan(fn func(key, value []byte) bool) error {
	return s.db.View(func(tx *buntdb.Tx) error {
		return tx.Ascend("", func(key, value string) bool {
			return fn([]byte(key), []byte(value))
		})
	})
}

func (s *buntDBStore) SeekGE(key []byte) ([]byte, []byte, bool, error) {
	var gotKey []byte
	var gotValue []byte
	err := s.db.View(func(tx *buntdb.Tx) error {
		return tx.AscendGreaterOrEqual("", string(key), func(key, value string) bool {
			gotKey = []byte(key)
			gotValue = []byte(value)
			return false
		})
	})
	return gotKey, gotValue, gotKey != nil, err
}

func (s *buntDBStore) Close() error {
	return s.db.Close()
}

type levelDBStore struct {
	db *leveldb.DB
}

func openLevelDBStore(_ *testing.B, dir string) (pointStore, error) {
	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		return nil, err
	}
	return &levelDBStore{db: db}, nil
}

func (s *levelDBStore) Put(key, value []byte) error {
	return s.db.Put(key, value, &opt.WriteOptions{Sync: false})
}

func (s *levelDBStore) Get(key []byte) ([]byte, bool, error) {
	value, err := s.db.Get(key, nil)
	if errors.Is(err, leveldberrors.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (s *levelDBStore) Delete(key []byte) error {
	return s.db.Delete(key, &opt.WriteOptions{Sync: false})
}

func (s *levelDBStore) Scan(fn func(key, value []byte) bool) error {
	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()
	for iter.First(); iter.Valid(); iter.Next() {
		if !fn(iter.Key(), iter.Value()) {
			break
		}
	}
	return iter.Error()
}

func (s *levelDBStore) SeekGE(key []byte) ([]byte, []byte, bool, error) {
	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()
	if !iter.Seek(key) {
		return nil, nil, false, iter.Error()
	}
	return cloneBytes(iter.Key()), cloneBytes(iter.Value()), true, iter.Error()
}

func (s *levelDBStore) Close() error {
	return s.db.Close()
}

type pebbleStore struct {
	db *pebble.DB
}

func openPebbleStore(_ *testing.B, dir string) (pointStore, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &pebbleStore{db: db}, nil
}

func (s *pebbleStore) Put(key, value []byte) error {
	return s.db.Set(key, value, pebble.NoSync)
}

func (s *pebbleStore) Get(key []byte) ([]byte, bool, error) {
	value, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	return cloneBytes(value), true, nil
}

func (s *pebbleStore) Delete(key []byte) error {
	return s.db.Delete(key, pebble.NoSync)
}

func (s *pebbleStore) Scan(fn func(key, value []byte) bool) error {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		if !fn(iter.Key(), iter.Value()) {
			break
		}
	}
	return iter.Error()
}

func (s *pebbleStore) SeekGE(key []byte) ([]byte, []byte, bool, error) {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return nil, nil, false, err
	}
	defer iter.Close()
	if !iter.SeekGE(key) {
		return nil, nil, false, iter.Error()
	}
	return cloneBytes(iter.Key()), cloneBytes(iter.Value()), true, iter.Error()
}

func (s *pebbleStore) Close() error {
	return s.db.Close()
}

type mapStore struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func openMapStore(_ *testing.B, _ string) (pointStore, error) {
	return &mapStore{m: make(map[string][]byte)}, nil
}

func (s *mapStore) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[string(key)] = cloneBytes(value)
	return nil
}

func (s *mapStore) Get(key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.m[string(key)]
	if !ok {
		return nil, false, nil
	}
	return cloneBytes(value), true, nil
}

func (s *mapStore) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, string(key))
	return nil
}

func (s *mapStore) Close() error {
	return nil
}
