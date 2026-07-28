package api

import (
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestCursorRoundTripAndTamper(t *testing.T) {
	codec := NewCursorCodec("secret")
	want := Cursor{CreatedAt: time.Now().UTC().Truncate(time.Nanosecond), ID: uuid.New()}
	raw, err := codec.Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := codec.Decode(raw)
	if err != nil || got != want {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if _, err := codec.Decode(raw + "x"); err == nil {
		t.Fatal("tampered cursor accepted")
	}
}
