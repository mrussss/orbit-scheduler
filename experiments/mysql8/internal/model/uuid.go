package model

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

func UUIDToBytes(id uuid.UUID) []byte {
	raw := make([]byte, len(id))
	copy(raw, id[:])
	return raw
}

func BytesToUUID(raw []byte) (uuid.UUID, error) {
	if len(raw) != 16 {
		return uuid.Nil, fmt.Errorf("binary UUID must contain 16 bytes, got %d", len(raw))
	}
	return uuid.FromBytes(raw)
}

type BinaryUUID [16]byte

func BinaryUUIDFrom(id uuid.UUID) BinaryUUID { return BinaryUUID(id) }
func (id BinaryUUID) UUID() uuid.UUID        { return uuid.UUID(id) }
func (id BinaryUUID) IsZero() bool           { return id == BinaryUUID{} }

func (id BinaryUUID) Value() (driver.Value, error) {
	if id.IsZero() {
		return nil, errors.New("zero UUID is not a valid database identifier")
	}
	return UUIDToBytes(id.UUID()), nil
}

func (id *BinaryUUID) Scan(value any) error {
	if id == nil {
		return errors.New("cannot scan UUID into nil receiver")
	}
	raw, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan binary UUID from %T", value)
	}
	parsed, err := BytesToUUID(raw)
	if err != nil {
		return err
	}
	*id = BinaryUUIDFrom(parsed)
	return nil
}
