package multi

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

type Multi struct {
	stores []storage.Storage
}

func New(stores []storage.Storage) *Multi {
	return &Multi{stores: stores}
}

func (m *Multi) Init(ctx context.Context) error {
	for _, s := range m.stores {
		if err := s.Init(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *Multi) EnsureDir(ctx context.Context, path string) error {
	return m.each(ctx, func(s storage.Storage) error { return s.EnsureDir(ctx, path) })
}

func (m *Multi) Upload(ctx context.Context, path string, data []byte) error {
	return m.pick(path).Upload(ctx, path, data)
}

func (m *Multi) Download(ctx context.Context, path string) ([]byte, error) {
	data, err := m.pick(path).Download(ctx, path)
	if err == nil {
		return data, nil
	}
	for _, s := range m.stores {
		data, err = s.Download(ctx, path)
		if err == nil {
			return data, nil
		}
	}
	return nil, err
}

func (m *Multi) List(ctx context.Context, dir string, prefix string) ([]storage.FileInfo, error) {
	type result struct {
		files []storage.FileInfo
		err   error
	}
	ch := make(chan result, len(m.stores))
	for _, s := range m.stores {
		go func(s storage.Storage) {
			files, err := s.List(ctx, dir, prefix)
			ch <- result{files: files, err: err}
		}(s)
	}
	seen := make(map[string]struct{})
	var out []storage.FileInfo
	var firstErr error
	for range m.stores {
		r := <-ch
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		for _, f := range r.files {
			key := f.Name
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, f)
		}
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func (m *Multi) Delete(ctx context.Context, path string) error {
	return m.each(ctx, func(s storage.Storage) error { return s.Delete(ctx, path) })
}

func (m *Multi) Watch(ctx context.Context, dir string, since time.Time, timeout time.Duration) ([]storage.FileInfo, error) {
	return m.List(ctx, dir, "")
}

func (m *Multi) BatchDelete(ctx context.Context, paths []string) error {
	return m.each(ctx, func(s storage.Storage) error {
		if bd, ok := s.(interface {
			BatchDelete(context.Context, []string) error
		}); ok {
			return bd.BatchDelete(ctx, paths)
		}
		for _, p := range paths {
			_ = s.Delete(ctx, p)
		}
		return nil
	})
}

func (m *Multi) pick(path string) storage.Storage {
	if len(m.stores) == 1 {
		return m.stores[0]
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(path))
	return m.stores[int(h.Sum32())%len(m.stores)]
}

func (m *Multi) each(ctx context.Context, fn func(storage.Storage) error) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(m.stores))
	for _, s := range m.stores {
		wg.Add(1)
		go func(s storage.Storage) {
			defer wg.Done()
			if err := fn(s); err != nil {
				errCh <- err
			}
		}(s)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}
