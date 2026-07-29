package model

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
)

func TestBinaryUUIDRoundTrip(t *testing.T) {
	id := uuid.New()
	raw := UUIDToBytes(id)
	if len(raw) != 16 || !bytes.Equal(raw, id[:]) {
		t.Fatalf("encoded UUID=%x", raw)
	}
	raw[0] ^= 0xff
	if id[0] == raw[0] {
		t.Fatal("UUIDToBytes returned aliased storage")
	}
	raw = UUIDToBytes(id)
	decoded, err := BytesToUUID(raw)
	if err != nil || decoded != id {
		t.Fatalf("decoded=%s err=%v", decoded, err)
	}
	if _, err := BytesToUUID(raw[:15]); err == nil {
		t.Fatal("short UUID accepted")
	}

	binary := BinaryUUIDFrom(id)
	value, err := binary.Value()
	if err != nil {
		t.Fatal(err)
	}
	var scanned BinaryUUID
	if err := scanned.Scan(value); err != nil || scanned.UUID() != id {
		t.Fatalf("scanned=%s err=%v", scanned.UUID(), err)
	}
	if _, err := (BinaryUUID{}).Value(); err == nil {
		t.Fatal("zero UUID accepted as an identifier")
	}
}
