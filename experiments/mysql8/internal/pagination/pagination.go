package pagination

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/model"
)

const MaxPageSize = 500

type Cursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        uuid.UUID `json:"id"`
}

type Item struct {
	ID        uuid.UUID
	Status    model.TaskStatus
	Priority  int
	CreatedAt time.Time
}

type Pager struct{ db *sql.DB }

func New(db *sql.DB) (*Pager, error) {
	if db == nil {
		return nil, errors.New("pagination database is required")
	}
	return &Pager{db: db}, nil
}

func (p *Pager) Offset(ctx context.Context, projectID uuid.UUID, status model.TaskStatus, limit, offset int) ([]Item, error) {
	if err := validate(projectID, status, limit); err != nil || offset < 0 {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("offset must not be negative")
	}
	return queryItems(ctx, p.db, OffsetSQL, model.UUIDToBytes(projectID), status, limit, offset)
}

func (p *Pager) Cursor(ctx context.Context, projectID uuid.UUID, status model.TaskStatus, limit int, after *Cursor) ([]Item, error) {
	if err := validate(projectID, status, limit); err != nil {
		return nil, err
	}
	if after == nil {
		return queryItems(ctx, p.db, FirstCursorSQL, model.UUIDToBytes(projectID), status, limit)
	}
	if after.ID == uuid.Nil || after.CreatedAt.IsZero() {
		return nil, errors.New("cursor is incomplete")
	}
	return queryItems(ctx, p.db, CursorSQL, model.UUIDToBytes(projectID), status, after.CreatedAt.UTC(), after.CreatedAt.UTC(), model.UUIDToBytes(after.ID), limit)
}

func Encode(cursor Cursor) (string, error) {
	if cursor.ID == uuid.Nil || cursor.CreatedAt.IsZero() {
		return "", errors.New("cursor is incomplete")
	}
	raw, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func Decode(encoded string) (Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return Cursor{}, fmt.Errorf("decode cursor: %w", err)
	}
	var cursor Cursor
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return Cursor{}, fmt.Errorf("decode cursor JSON: %w", err)
	}
	if cursor.ID == uuid.Nil || cursor.CreatedAt.IsZero() {
		return Cursor{}, errors.New("cursor is incomplete")
	}
	return cursor, nil
}

const OffsetSQL = `SELECT id,status,priority,created_at
FROM lab_tasks
WHERE project_id=? AND status=?
ORDER BY created_at DESC,id DESC
LIMIT ? OFFSET ?`

const FirstCursorSQL = `SELECT id,status,priority,created_at
FROM lab_tasks
WHERE project_id=? AND status=?
ORDER BY created_at DESC,id DESC
LIMIT ?`

const CursorSQL = `SELECT id,status,priority,created_at
FROM lab_tasks
WHERE project_id=? AND status=?
  AND (created_at < ? OR (created_at = ? AND id < ?))
ORDER BY created_at DESC,id DESC
LIMIT ?`

func validate(projectID uuid.UUID, status model.TaskStatus, limit int) error {
	if projectID == uuid.Nil || status == "" || limit <= 0 || limit > MaxPageSize {
		return errors.New("invalid pagination arguments")
	}
	return nil
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func queryItems(ctx context.Context, db queryer, query string, args ...any) ([]Item, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Item, 0)
	for rows.Next() {
		var rawID []byte
		var item Item
		if err := rows.Scan(&rawID, &item.Status, &item.Priority, &item.CreatedAt); err != nil {
			return nil, err
		}
		item.ID, err = model.BytesToUUID(rawID)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
