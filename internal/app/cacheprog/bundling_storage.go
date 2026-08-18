package cacheprog

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // presence detection checksum, same as handler
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/platacard/cacheprog/internal/infra/logging"
)

// Flusher is an optional extension for RemoteStorage implementations that
// buffer writes and must drain them before process exit.
type Flusher interface {
	Flush(ctx context.Context) error
}

// BundlingOptions configures write-side packing of cache entries.
type BundlingOptions struct {
	// TargetSize is the soft size threshold that triggers a flush.
	// If <= 0, bundling is disabled (NewBundlingRemoteStorage returns upstream).
	TargetSize int64
	// MaxEntries flushes once this many segments are buffered (0 = unlimited).
	MaxEntries int
	// IdleFlush is an optional idle timer that flushes a non-empty buffer.
	// Zero disables idle flushes (size/entry/close only).
	IdleFlush time.Duration
}

type pendingSegment struct {
	ActionID             []byte
	OutputID             []byte
	Body                 []byte
	UncompressedSize     int64
	CompressionAlgorithm string
}

// BundlingRemoteStorage batches Put requests into bundles and, on Get,
// downloads the whole bundle and hydrates sibling segments into LocalStorage.
type BundlingRemoteStorage struct {
	upstream RemoteStorage
	local    LocalStorage
	codec    CompressionCodec
	opts     BundlingOptions

	mu        sync.Mutex
	pending   []pendingSegment
	payload   []byte
	closed    bool
	idleTimer *time.Timer

	flushWG sync.WaitGroup

	// inFlight deduplicates concurrent downloads of the same bundle.
	inFlight sync.Map // hex(bundleID) -> *bundleFlight
}

type bundleFlight struct {
	once    sync.Once
	index   *bundleIndex
	payload []byte
	err     error
}

// NewBundlingRemoteStorage wraps upstream with Depot-style bundling.
// Returns upstream unchanged when TargetSize <= 0 or required deps are missing.
func NewBundlingRemoteStorage(upstream RemoteStorage, local LocalStorage, codec CompressionCodec, opts BundlingOptions) RemoteStorage {
	if upstream == nil || opts.TargetSize <= 0 || local == nil || codec == nil {
		return upstream
	}
	return &BundlingRemoteStorage{
		upstream: upstream,
		local:    local,
		codec:    codec,
		opts:     opts,
	}
}

func (b *BundlingRemoteStorage) Put(ctx context.Context, request *PutRequest) (*PutResponse, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, fmt.Errorf("read put body: %w", err)
	}
	if request.Size > 0 && int64(len(body)) != request.Size {
		return nil, fmt.Errorf("put body size mismatch: got %d want %d", len(body), request.Size)
	}

	// Oversized entries bypass the buffer: flush current buffer, then store alone.
	if int64(len(body)) >= b.opts.TargetSize {
		if err := b.flushLockedTake(ctx); err != nil {
			return nil, err
		}
		return b.putDirect(ctx, request, body)
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return b.putDirect(ctx, request, body)
	}

	b.pending = append(b.pending, pendingSegment{
		ActionID:             append([]byte(nil), request.ActionID...),
		OutputID:             append([]byte(nil), request.OutputID...),
		Body:                 body,
		UncompressedSize:     request.UncompressedSize,
		CompressionAlgorithm: request.CompressionAlgorithm,
	})
	b.payload = append(b.payload, body...)
	shouldFlush := int64(len(b.payload)) >= b.opts.TargetSize ||
		(b.opts.MaxEntries > 0 && len(b.pending) >= b.opts.MaxEntries)

	var flushPending []pendingSegment
	var flushPayload []byte
	if shouldFlush {
		flushPending = b.pending
		flushPayload = b.payload
		b.pending = nil
		b.payload = nil
		if b.idleTimer != nil {
			b.idleTimer.Stop()
			b.idleTimer = nil
		}
	} else {
		b.resetIdleTimerLocked()
	}
	b.mu.Unlock()

	if shouldFlush {
		return &PutResponse{}, b.flushAsync(ctx, flushPending, flushPayload)
	}
	return &PutResponse{}, nil
}

func (b *BundlingRemoteStorage) Get(ctx context.Context, request *GetRequest) (*GetResponse, error) {
	obj, err := b.upstream.Get(ctx, request)
	if err != nil {
		return nil, err
	}

	if !isBundlePointer(obj.CompressionAlgorithm) {
		return obj, nil
	}

	defer obj.Body.Close()
	ptrBody, err := io.ReadAll(obj.Body)
	if err != nil {
		return nil, fmt.Errorf("read bundle pointer: %w", err)
	}
	ptr, err := decodeBundlePointer(ptrBody)
	if err != nil {
		return nil, err
	}

	index, payload, err := b.loadBundle(ctx, ptr.BundleID)
	if err != nil {
		return nil, err
	}

	var target *bundleSegment
	var targetData []byte
	for i := range index.Segments {
		seg := &index.Segments[i]
		segData := payload[seg.Offset : seg.Offset+seg.Length]
		if bytes.Equal(seg.ActionID, request.ActionID) {
			target = seg
			targetData = segData
		}
		b.prefetchSegment(ctx, seg, segData, request.ActionID)
	}

	if target == nil {
		return nil, fmt.Errorf("action id not found in bundle: %w", ErrNotFound)
	}

	return &GetResponse{
		OutputID:             append([]byte(nil), target.OutputID...),
		ModTime:              obj.ModTime,
		Size:                 target.Length,
		Body:                 io.NopCloser(bytes.NewReader(targetData)),
		CompressionAlgorithm: target.CompressionAlgorithm,
		UncompressedSize:     target.UncompressedSize,
	}, nil
}

// Flush uploads any buffered segments and waits for in-flight flushes.
func (b *BundlingRemoteStorage) Flush(ctx context.Context) error {
	b.mu.Lock()
	b.closed = true
	if b.idleTimer != nil {
		b.idleTimer.Stop()
		b.idleTimer = nil
	}
	b.mu.Unlock()

	err := b.flushLockedTake(ctx)
	b.flushWG.Wait()
	return err
}

func (b *BundlingRemoteStorage) putDirect(ctx context.Context, request *PutRequest, body []byte) (*PutResponse, error) {
	req := *request
	req.Body = bytes.NewReader(body)
	req.Size = int64(len(body))
	return b.upstream.Put(ctx, &req)
}

func (b *BundlingRemoteStorage) flushAsync(ctx context.Context, pending []pendingSegment, payload []byte) error {
	b.flushWG.Add(1)
	go func() {
		defer b.flushWG.Done()
		if err := b.flushSegments(context.WithoutCancel(ctx), pending, payload); err != nil {
			slog.ErrorContext(ctx, "Bundle flush failed", logging.Error(err))
		}
	}()
	return nil
}

func (b *BundlingRemoteStorage) flushLockedTake(ctx context.Context) error {
	b.mu.Lock()
	pending := b.pending
	payload := b.payload
	b.pending = nil
	b.payload = nil
	if b.idleTimer != nil {
		b.idleTimer.Stop()
		b.idleTimer = nil
	}
	b.mu.Unlock()

	if len(pending) == 0 {
		return nil
	}
	return b.flushSegments(ctx, pending, payload)
}

func (b *BundlingRemoteStorage) flushSegments(ctx context.Context, pending []pendingSegment, payload []byte) error {
	bundleID := make([]byte, 32)
	if _, err := rand.Read(bundleID); err != nil {
		return fmt.Errorf("generate bundle id: %w", err)
	}

	segments := make([]bundleSegment, 0, len(pending))
	var offset int64
	for _, p := range pending {
		segments = append(segments, bundleSegment{
			ActionID:             p.ActionID,
			OutputID:             p.OutputID,
			Offset:               offset,
			Length:               int64(len(p.Body)),
			UncompressedSize:     p.UncompressedSize,
			CompressionAlgorithm: p.CompressionAlgorithm,
		})
		offset += int64(len(p.Body))
	}

	bundleBody, err := encodeBundle(segments, payload)
	if err != nil {
		return err
	}

	if err := b.putBytes(ctx, &PutRequest{
		ActionID:             bundleID,
		OutputID:             bundleID,
		Size:                 int64(len(bundleBody)),
		Body:                 bytes.NewReader(bundleBody),
		CompressionAlgorithm: BundleAlgorithm,
		UncompressedSize:     int64(len(bundleBody)),
	}, bundleBody); err != nil {
		return fmt.Errorf("put bundle: %w", err)
	}

	var firstErr error
	for _, p := range pending {
		ptrBody, err := encodeBundlePointer(bundleID)
		if err != nil {
			firstErr = errors.Join(firstErr, err)
			continue
		}
		if err := b.putBytes(ctx, &PutRequest{
			ActionID:             p.ActionID,
			OutputID:             p.OutputID,
			Size:                 int64(len(ptrBody)),
			Body:                 bytes.NewReader(ptrBody),
			CompressionAlgorithm: BundlePointerAlgorithm,
			UncompressedSize:     int64(len(ptrBody)),
		}, ptrBody); err != nil {
			firstErr = errors.Join(firstErr, fmt.Errorf("put pointer for %x: %w", p.ActionID, err))
		}
	}
	return firstErr
}

func (b *BundlingRemoteStorage) putBytes(ctx context.Context, req *PutRequest, body []byte) error {
	md5sum := md5.Sum(body) //nolint:gosec
	sha := sha256.Sum256(body)
	req.MD5Sum = md5sum[:]
	req.Sha256Sum = sha[:]
	req.Body = bytes.NewReader(body)
	_, err := b.upstream.Put(ctx, req)
	return err
}

func (b *BundlingRemoteStorage) loadBundle(ctx context.Context, bundleID []byte) (*bundleIndex, []byte, error) {
	key := hex.EncodeToString(bundleID)
	flightAny, _ := b.inFlight.LoadOrStore(key, &bundleFlight{})
	flight := flightAny.(*bundleFlight)

	flight.once.Do(func() {
		obj, err := b.upstream.Get(ctx, &GetRequest{ActionID: bundleID})
		if err != nil {
			flight.err = fmt.Errorf("get bundle %s: %w", key, err)
			return
		}
		defer obj.Body.Close()
		if !isBundleObject(obj.CompressionAlgorithm) {
			flight.err = fmt.Errorf("unexpected bundle algorithm %q", obj.CompressionAlgorithm)
			return
		}
		data, err := io.ReadAll(obj.Body)
		if err != nil {
			flight.err = fmt.Errorf("read bundle %s: %w", key, err)
			return
		}
		index, payload, err := decodeBundle(data)
		if err != nil {
			flight.err = err
			return
		}
		flight.index = index
		flight.payload = payload
	})

	if flight.err != nil {
		b.inFlight.Delete(key)
		return nil, nil, flight.err
	}
	return flight.index, flight.payload, nil
}

func (b *BundlingRemoteStorage) prefetchSegment(ctx context.Context, seg *bundleSegment, data []byte, requested []byte) {
	if bytes.Equal(seg.ActionID, requested) {
		return // handler will materialize the requested object
	}

	_, err := b.local.GetLocal(ctx, &LocalGetRequest{ActionID: seg.ActionID})
	if err == nil {
		return
	}
	if !errors.Is(err, ErrNotFound) {
		slog.DebugContext(ctx, "Prefetch skip: local get error", logging.Error(err))
		return
	}

	decompressed, err := b.codec.Decompress(&DecompressRequest{
		Body:      io.NopCloser(bytes.NewReader(data)),
		Algorithm: seg.CompressionAlgorithm,
	})
	if err != nil {
		slog.DebugContext(ctx, "Prefetch decompress failed", logging.Error(err))
		return
	}
	defer decompressed.Body.Close()

	_, err = b.local.PutLocal(ctx, &LocalPutRequest{
		ActionID: seg.ActionID,
		OutputID: seg.OutputID,
		Size:     seg.UncompressedSize,
		Body:     decompressed.Body,
	})
	if err != nil {
		slog.DebugContext(ctx, "Prefetch put local failed", logging.Error(err))
	}
}

func (b *BundlingRemoteStorage) resetIdleTimerLocked() {
	if b.opts.IdleFlush <= 0 {
		return
	}
	if b.idleTimer != nil {
		b.idleTimer.Stop()
	}
	b.idleTimer = time.AfterFunc(b.opts.IdleFlush, func() {
		b.flushWG.Add(1)
		defer b.flushWG.Done()
		if err := b.flushLockedTake(context.Background()); err != nil {
			slog.Error("Idle bundle flush failed", logging.Error(err))
		}
	})
}
