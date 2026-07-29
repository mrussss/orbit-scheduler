package pagination

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursorCodecRoundTrip(t *testing.T) {
	want := Cursor{CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC), ID: uuid.New()}
	encoded, err := Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(encoded)
	if err != nil || got.ID != want.ID || !got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("cursor=%+v err=%v", got, err)
	}
	if _, err := Decode("not-base64"); err == nil {
		t.Fatal("invalid cursor accepted")
	}
}
