package cacheprog

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeBundle(t *testing.T) {
	segments := []bundleSegment{
		{
			ActionID:             []byte("a1"),
			OutputID:             []byte("o1"),
			Offset:               0,
			Length:               3,
			UncompressedSize:     3,
			CompressionAlgorithm: "",
		},
		{
			ActionID:             []byte("a2"),
			OutputID:             []byte("o2"),
			Offset:               3,
			Length:               5,
			UncompressedSize:     5,
			CompressionAlgorithm: "zstd",
		},
	}
	payload := []byte("foobarbaz!")

	encoded, err := encodeBundle(segments, payload[:8])
	require.NoError(t, err)

	index, decodedPayload, err := decodeBundle(encoded)
	require.NoError(t, err)
	assert.Equal(t, bundleVersion, index.Version)
	require.Len(t, index.Segments, 2)
	assert.Equal(t, []byte("a1"), index.Segments[0].ActionID)
	assert.Equal(t, []byte("a2"), index.Segments[1].ActionID)
	assert.Equal(t, payload[:8], decodedPayload)
}

func TestDecodeBundleRejectsBadMagic(t *testing.T) {
	_, _, err := decodeBundle([]byte("XXXX\x00\x00\x00\x02{}"))
	require.Error(t, err)
}

func TestEncodeDecodeBundlePointer(t *testing.T) {
	id := []byte{1, 2, 3, 4}
	raw, err := encodeBundlePointer(id)
	require.NoError(t, err)

	ptr, err := decodeBundlePointer(raw)
	require.NoError(t, err)
	assert.Equal(t, id, ptr.BundleID)
}

func TestDecodeBundlePointerEmpty(t *testing.T) {
	_, err := decodeBundlePointer([]byte(`{"bundleID":""}`))
	require.Error(t, err)
}

func TestIsBundleMarkers(t *testing.T) {
	assert.True(t, isBundlePointer(BundlePointerAlgorithm))
	assert.False(t, isBundlePointer("zstd"))
	assert.True(t, isBundleObject(BundleAlgorithm))
	assert.False(t, isBundleObject(""))
}

func TestDecodeBundleSegmentBounds(t *testing.T) {
	segments := []bundleSegment{{
		ActionID: []byte("a"),
		Offset:   0,
		Length:   10,
	}}
	encoded, err := encodeBundle(segments, []byte("short"))
	require.NoError(t, err)

	// Corrupt length so segment extends past payload.
	encoded = bytes.Replace(encoded, []byte(`"length":10`), []byte(`"length":99`), 1)
	_, _, err = decodeBundle(encoded)
	require.Error(t, err)
}
