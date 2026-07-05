package transport

import (
	"bytes"
	"testing"
)

func TestEncodePayloadSkipsCompressionForLargeChunks(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), maxCompressInput+1)

	encoded := encodePayload(payload)
	if len(encoded) != len(payload)+1 {
		t.Fatalf("large payload should be emitted raw: got %d bytes, want %d", len(encoded), len(payload)+1)
	}
	if encoded[0] != headerRaw {
		t.Fatalf("large payload header = %d, want raw header %d", encoded[0], headerRaw)
	}

	decoded, err := decodePayload(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatal("decoded payload does not match original")
	}
}

func TestEncodePayloadRoundTripSmallPayload(t *testing.T) {
	payload := bytes.Repeat([]byte("hello world "), 1024)

	encoded := encodePayload(payload)
	decoded, err := decodePayload(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatal("decoded payload does not match original")
	}
}
