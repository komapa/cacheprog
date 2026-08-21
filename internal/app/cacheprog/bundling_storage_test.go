package cacheprog

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/platacard/cacheprog/internal/infra/cacheproto"
)

type memRemote struct {
	mu   sync.Mutex
	objs map[string]*memObj
}

type memObj struct {
	outputID             []byte
	body                 []byte
	compressionAlgorithm string
	uncompressedSize     int64
	modTime              time.Time
}

func newMemRemote() *memRemote {
	return &memRemote{objs: make(map[string]*memObj)}
}

func (m *memRemote) key(actionID []byte) string {
	return hex.EncodeToString(actionID)
}

func (m *memRemote) Get(_ context.Context, request *GetRequest) (*GetResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objs[m.key(request.ActionID)]
	if !ok {
		return nil, ErrNotFound
	}
	return &GetResponse{
		OutputID:             append([]byte(nil), obj.outputID...),
		ModTime:              obj.modTime,
		Size:                 int64(len(obj.body)),
		Body:                 io.NopCloser(bytes.NewReader(obj.body)),
		CompressionAlgorithm: obj.compressionAlgorithm,
		UncompressedSize:     obj.uncompressedSize,
	}, nil
}

func (m *memRemote) Put(_ context.Context, request *PutRequest) (*PutResponse, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[m.key(request.ActionID)] = &memObj{
		outputID:             append([]byte(nil), request.OutputID...),
		body:                 body,
		compressionAlgorithm: request.CompressionAlgorithm,
		uncompressedSize:     request.UncompressedSize,
		modTime:              time.Unix(1, 0).UTC(),
	}
	return &PutResponse{}, nil
}

func (m *memRemote) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.objs)
}

func (m *memRemote) countByAlgo(algo string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, o := range m.objs {
		if o.compressionAlgorithm == algo {
			n++
		}
	}
	return n
}

type memLocal struct {
	mu   sync.Mutex
	objs map[string]*memLocalObj
}

type memLocalObj struct {
	outputID []byte
	body     []byte
	modTime  time.Time
}

func newMemLocal() *memLocal {
	return &memLocal{objs: make(map[string]*memLocalObj)}
}

func (m *memLocal) GetLocal(_ context.Context, request *LocalGetRequest) (*LocalGetResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objs[hex.EncodeToString(request.ActionID)]
	if !ok {
		return nil, ErrNotFound
	}
	return &LocalGetResponse{
		OutputID: append([]byte(nil), obj.outputID...),
		ModTime:  obj.modTime,
		Size:     int64(len(obj.body)),
		DiskPath: "/tmp/" + hex.EncodeToString(request.ActionID),
	}, nil
}

func (m *memLocal) GetLocalObject(_ context.Context, request *LocalObjectGetRequest) (*LocalObjectGetResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objs[hex.EncodeToString(request.ActionID)]
	if !ok {
		return nil, ErrNotFound
	}
	return &LocalObjectGetResponse{
		ActionID: append([]byte(nil), request.ActionID...),
		OutputID: append([]byte(nil), obj.outputID...),
		Size:     int64(len(obj.body)),
		Body:     seekNopCloser{bytes.NewReader(obj.body)},
	}, nil
}

func (m *memLocal) PutLocal(_ context.Context, request *LocalPutRequest) (*LocalPutResponse, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[hex.EncodeToString(request.ActionID)] = &memLocalObj{
		outputID: append([]byte(nil), request.OutputID...),
		body:     body,
		modTime:  time.Unix(2, 0).UTC(),
	}
	return &LocalPutResponse{DiskPath: "/tmp/" + hex.EncodeToString(request.ActionID)}, nil
}

func (m *memLocal) has(actionID []byte) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objs[hex.EncodeToString(actionID)]
	return ok
}

type passthroughCodec struct{}

func (passthroughCodec) Compress(req *CompressRequest) (*CompressResponse, error) {
	data, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	if seeker, ok := req.Body.(io.Seeker); ok {
		_, _ = seeker.Seek(0, io.SeekStart)
	}
	return &CompressResponse{
		Size:      int64(len(data)),
		Body:      seekNopCloser{bytes.NewReader(data)},
		Algorithm: "",
	}, nil
}

func (passthroughCodec) Decompress(req *DecompressRequest) (*DecompressResponse, error) {
	return &DecompressResponse{Body: req.Body}, nil
}

type seekNopCloser struct {
	*bytes.Reader
}

func (seekNopCloser) Close() error { return nil }

func TestNewBundlingRemoteStorageDisabled(t *testing.T) {
	upstream := newMemRemote()
	got := NewBundlingRemoteStorage(upstream, newMemLocal(), passthroughCodec{}, BundlingOptions{TargetSize: 0})
	assert.Equal(t, RemoteStorage(upstream), got)
}

func TestBundlingRemoteStorage_FlushOnClose(t *testing.T) {
	upstream := newMemRemote()
	local := newMemLocal()
	b := NewBundlingRemoteStorage(upstream, local, passthroughCodec{}, BundlingOptions{
		TargetSize: 1024,
		MaxEntries: 100,
	}).(*BundlingRemoteStorage)

	_, err := b.Put(context.Background(), &PutRequest{
		ActionID: []byte("act-1"),
		OutputID: []byte("out-1"),
		Size:     4,
		Body:     bytes.NewReader([]byte("data")),
	})
	require.NoError(t, err)
	assert.Equal(t, 0, upstream.len(), "should buffer until flush")

	require.NoError(t, b.Flush(context.Background()))
	assert.Equal(t, 1, upstream.countByAlgo(BundleAlgorithm))
	assert.Equal(t, 1, upstream.countByAlgo(BundlePointerAlgorithm))

	got, err := b.Get(context.Background(), &GetRequest{ActionID: []byte("act-1")})
	require.NoError(t, err)
	defer got.Body.Close()
	body, err := io.ReadAll(got.Body)
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), body)
	assert.Equal(t, []byte("out-1"), got.OutputID)
}

func TestBundlingRemoteStorage_FlushByMaxEntries(t *testing.T) {
	upstream := newMemRemote()
	b := NewBundlingRemoteStorage(upstream, newMemLocal(), passthroughCodec{}, BundlingOptions{
		TargetSize: 1 << 20,
		MaxEntries: 2,
	}).(*BundlingRemoteStorage)

	_, err := b.Put(context.Background(), &PutRequest{
		ActionID: []byte("a"), OutputID: []byte("oa"), Size: 1, Body: bytes.NewReader([]byte("1")),
	})
	require.NoError(t, err)
	_, err = b.Put(context.Background(), &PutRequest{
		ActionID: []byte("b"), OutputID: []byte("ob"), Size: 1, Body: bytes.NewReader([]byte("2")),
	})
	require.NoError(t, err)

	require.NoError(t, b.Flush(context.Background()))
	assert.Equal(t, 1, upstream.countByAlgo(BundleAlgorithm))
	assert.Equal(t, 2, upstream.countByAlgo(BundlePointerAlgorithm))
}

func TestBundlingRemoteStorage_PrefetchSiblings(t *testing.T) {
	upstream := newMemRemote()
	local := newMemLocal()
	b := NewBundlingRemoteStorage(upstream, local, passthroughCodec{}, BundlingOptions{
		TargetSize: 1024,
		MaxEntries: 2,
	}).(*BundlingRemoteStorage)

	_, err := b.Put(context.Background(), &PutRequest{
		ActionID: []byte("sib-1"), OutputID: []byte("o1"), Size: 3, Body: bytes.NewReader([]byte("one")), UncompressedSize: 3,
	})
	require.NoError(t, err)
	_, err = b.Put(context.Background(), &PutRequest{
		ActionID: []byte("sib-2"), OutputID: []byte("o2"), Size: 3, Body: bytes.NewReader([]byte("two")), UncompressedSize: 3,
	})
	require.NoError(t, err)
	require.NoError(t, b.Flush(context.Background()))

	got, err := b.Get(context.Background(), &GetRequest{ActionID: []byte("sib-1")})
	require.NoError(t, err)
	got.Body.Close()

	assert.True(t, local.has([]byte("sib-2")), "sibling should be prefetched into local storage")
	assert.False(t, local.has([]byte("sib-1")), "requested object is left for the handler")
}

func TestBundlingRemoteStorage_LegacyPassthrough(t *testing.T) {
	upstream := newMemRemote()
	b := NewBundlingRemoteStorage(upstream, newMemLocal(), passthroughCodec{}, BundlingOptions{
		TargetSize: 1024,
	}).(*BundlingRemoteStorage)

	_, err := upstream.Put(context.Background(), &PutRequest{
		ActionID:             []byte("legacy"),
		OutputID:             []byte("out"),
		Size:                 3,
		Body:                 bytes.NewReader([]byte("old")),
		CompressionAlgorithm: "zstd",
		UncompressedSize:     3,
	})
	require.NoError(t, err)

	got, err := b.Get(context.Background(), &GetRequest{ActionID: []byte("legacy")})
	require.NoError(t, err)
	defer got.Body.Close()
	body, err := io.ReadAll(got.Body)
	require.NoError(t, err)
	assert.Equal(t, []byte("old"), body)
	assert.Equal(t, "zstd", got.CompressionAlgorithm)
}

func TestBundlingRemoteStorage_OversizedDirectPut(t *testing.T) {
	upstream := newMemRemote()
	b := NewBundlingRemoteStorage(upstream, newMemLocal(), passthroughCodec{}, BundlingOptions{
		TargetSize: 4,
	}).(*BundlingRemoteStorage)

	body := []byte("larger-than-target")
	_, err := b.Put(context.Background(), &PutRequest{
		ActionID: []byte("big"),
		OutputID: []byte("ob"),
		Size:     int64(len(body)),
		Body:     bytes.NewReader(body),
	})
	require.NoError(t, err)
	assert.Equal(t, 1, upstream.len())
	assert.Equal(t, 0, upstream.countByAlgo(BundleAlgorithm))
}

func TestHandler_FlushBundlingOnClose(t *testing.T) {
	upstream := newMemRemote()
	local := newMemLocal()
	bundled := NewBundlingRemoteStorage(upstream, local, passthroughCodec{}, BundlingOptions{
		TargetSize: 1024,
	})

	h := NewHandler(HandlerOptions{
		RemoteStorage:    bundled,
		LocalStorage:     local,
		CompressionCodec: passthroughCodec{},
		CloseTimeout:     time.Second,
	})

	writer := &mockResponseWriter{}
	h.Handle(context.Background(), writer, &cacheproto.Request{
		ID:       1,
		Command:  cacheproto.CmdPut,
		ActionID: []byte("close-1"),
		OutputID: []byte("out"),
		BodySize: 4,
		Body:     bytes.NewReader([]byte("abcd")),
	})

	h.Handle(context.Background(), writer, &cacheproto.Request{
		ID:      2,
		Command: cacheproto.CmdClose,
	})

	assert.Equal(t, 1, upstream.countByAlgo(BundleAlgorithm))
	assert.Equal(t, 1, upstream.countByAlgo(BundlePointerAlgorithm))
}
