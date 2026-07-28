package pgstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func (s *Store) RegisterWorker(ctx context.Context, worker domain.WorkerInstance) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO worker_instances(id,worker_name,hostname,capacity,supported_task_types,running_tasks,draining,last_heartbeat_at,started_at,process_version,metadata,created_at,updated_at)VALUES($1,$2,$3,$4,$5,0,false,statement_timestamp(),statement_timestamp(),$6,$7,statement_timestamp(),statement_timestamp())`, worker.ID, worker.WorkerName, worker.Hostname, worker.Capacity, worker.SupportedTaskTypes, worker.ProcessVersion, defaultObject(worker.Metadata))
	if err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	return nil
}
func (s *Store) HeartbeatWorker(ctx context.Context, instanceID uuid.UUID, running int, draining bool) error {
	command, err := s.pool.Exec(ctx, `UPDATE worker_instances SET running_tasks=$2,draining=$3,last_heartbeat_at=statement_timestamp(),updated_at=statement_timestamp() WHERE id=$1`, instanceID, running, draining)
	if err != nil {
		return fmt.Errorf("heartbeat worker: %w", err)
	}
	if command.RowsAffected() == 0 {
		return scheduler.ErrWorkerNotFound
	}
	return nil
}
func (s *Store) SetWorkerDraining(ctx context.Context, instanceID uuid.UUID, draining bool) error {
	command, err := s.pool.Exec(ctx, `UPDATE worker_instances SET draining=$2,last_heartbeat_at=statement_timestamp(),updated_at=statement_timestamp() WHERE id=$1`, instanceID, draining)
	if err != nil {
		return err
	}
	if command.RowsAffected() == 0 {
		return scheduler.ErrWorkerNotFound
	}
	return nil
}
func (s *Store) WorkerServerTime(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := s.pool.QueryRow(ctx, `SELECT statement_timestamp()`).Scan(&now)
	if err == pgx.ErrNoRows {
		return time.Time{}, scheduler.ErrWorkerNotFound
	}
	return now, err
}
func defaultObject(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}
