package cacheprog

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	// BundlePointerAlgorithm marks a remote object whose body is a JSON pointer
	// to a bundle rather than a standalone cache entry.
	BundlePointerAlgorithm = "bundle-ptr/v1"

	// BundleAlgorithm marks a remote object that stores concatenated cache
	// segments with a leading index.
	BundleAlgorithm = "bundle/v1"

	bundleMagic   = "CPB1"
	bundleVersion = 1
	maxIndexBytes = 16 << 20 // 16 MiB index cap
)

// bundlePointer is stored at the ActionID key and locates the owning bundle.
type bundlePointer struct {
	BundleID []byte `json:"bundleID"`
}

// bundleIndex describes segments packed into a bundle payload.
type bundleIndex struct {
	Version  int             `json:"v"`
	Segments []bundleSegment `json:"segments"`
}

type bundleSegment struct {
	ActionID             []byte `json:"actionID"`
	OutputID             []byte `json:"outputID"`
	Offset               int64  `json:"offset"`
	Length               int64  `json:"length"`
	UncompressedSize     int64  `json:"uncompressedSize"`
	CompressionAlgorithm string `json:"compressionAlgorithm"`
}

func encodeBundlePointer(bundleID []byte) ([]byte, error) {
	return json.Marshal(bundlePointer{BundleID: bundleID})
}

func decodeBundlePointer(data []byte) (*bundlePointer, error) {
	var ptr bundlePointer
	if err := json.Unmarshal(data, &ptr); err != nil {
		return nil, fmt.Errorf("decode bundle pointer: %w", err)
	}
	if len(ptr.BundleID) == 0 {
		return nil, fmt.Errorf("decode bundle pointer: empty bundle id")
	}
	return &ptr, nil
}

// encodeBundle packs an index and concatenated segment bodies into a single blob.
// Layout: magic(4) | indexLen(uint32 BE) | indexJSON | payload.
func encodeBundle(segments []bundleSegment, payload []byte) ([]byte, error) {
	index := bundleIndex{Version: bundleVersion, Segments: segments}
	indexJSON, err := json.Marshal(index)
	if err != nil {
		return nil, fmt.Errorf("encode bundle index: %w", err)
	}
	if len(indexJSON) > maxIndexBytes {
		return nil, fmt.Errorf("bundle index too large: %d", len(indexJSON))
	}

	out := make([]byte, 0, 4+4+len(indexJSON)+len(payload))
	out = append(out, bundleMagic...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(indexJSON)))
	out = append(out, indexJSON...)
	out = append(out, payload...)
	return out, nil
}

func decodeBundle(data []byte) (*bundleIndex, []byte, error) {
	if len(data) < 8 {
		return nil, nil, fmt.Errorf("bundle too short: %d", len(data))
	}
	if string(data[:4]) != bundleMagic {
		return nil, nil, fmt.Errorf("invalid bundle magic %q", data[:4])
	}
	indexLen := binary.BigEndian.Uint32(data[4:8])
	if indexLen == 0 || indexLen > maxIndexBytes {
		return nil, nil, fmt.Errorf("invalid bundle index length: %d", indexLen)
	}
	if len(data) < int(8+indexLen) {
		return nil, nil, fmt.Errorf("bundle truncated: need %d have %d", 8+indexLen, len(data))
	}

	var index bundleIndex
	if err := json.Unmarshal(data[8:8+indexLen], &index); err != nil {
		return nil, nil, fmt.Errorf("decode bundle index: %w", err)
	}
	if index.Version != bundleVersion {
		return nil, nil, fmt.Errorf("unsupported bundle version: %d", index.Version)
	}

	payload := data[8+indexLen:]
	for i, seg := range index.Segments {
		if seg.Offset < 0 || seg.Length < 0 {
			return nil, nil, fmt.Errorf("segment %d: negative offset/length", i)
		}
		end := seg.Offset + seg.Length
		if end < seg.Offset || end > int64(len(payload)) {
			return nil, nil, fmt.Errorf("segment %d: out of bounds [%d,%d) payload=%d", i, seg.Offset, end, len(payload))
		}
	}

	return &index, payload, nil
}

func isBundlePointer(algo string) bool {
	return algo == BundlePointerAlgorithm
}

func isBundleObject(algo string) bool {
	return algo == BundleAlgorithm
}
