package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/model"
	"gorm.io/gorm"
)

type Repository struct{ db *gorm.DB }

func New(db *gorm.DB) (*Repository, error) {
	if db == nil {
		return nil, errors.New("mysql lab gorm database is required")
	}
	return &Repository{db: db}, nil
}

func (r *Repository) CreateProject(ctx context.Context, project model.Project) error {
	if project.ID.IsZero() || project.Name == "" || len(project.Name) > 128 || !validProjectStatus(project.Status) {
		return ErrInvalid
	}
	return mapError(r.db.WithContext(ctx).Create(&project).Error)
}

func (r *Repository) GetProject(ctx context.Context, id uuid.UUID) (model.Project, error) {
	if id == uuid.Nil {
		return model.Project{}, ErrInvalid
	}
	var project model.Project
	err := r.db.WithContext(ctx).Where("id = ?", model.BinaryUUIDFrom(id)).First(&project).Error
	return project, mapError(err)
}

func (r *Repository) UpdateProject(ctx context.Context, id uuid.UUID, name string, status model.ProjectStatus, updatedAt time.Time) error {
	if id == uuid.Nil || name == "" || len(name) > 128 || !validProjectStatus(status) || updatedAt.IsZero() {
		return ErrInvalid
	}
	result := r.db.WithContext(ctx).Model(&model.Project{}).Where("id = ?", model.BinaryUUIDFrom(id)).Updates(map[string]any{"name": name, "status": status, "updated_at": updatedAt.UTC()})
	if result.Error != nil {
		return mapError(result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) DeleteProject(ctx context.Context, id uuid.UUID) error {
	result := r.db.WithContext(ctx).Where("id = ?", model.BinaryUUIDFrom(id)).Delete(&model.Project{})
	if result.Error != nil {
		return mapError(result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) CreateTask(ctx context.Context, task model.Task) error {
	if err := validateTask(task); err != nil {
		return err
	}
	return mapError(r.db.WithContext(ctx).Create(&task).Error)
}

func (r *Repository) GetTask(ctx context.Context, id uuid.UUID) (model.Task, error) {
	if id == uuid.Nil {
		return model.Task{}, ErrInvalid
	}
	var task model.Task
	err := r.db.WithContext(ctx).Where("id = ?", model.BinaryUUIDFrom(id)).First(&task).Error
	return task, mapError(err)
}

func (r *Repository) CompleteTask(ctx context.Context, id uuid.UUID, resultJSON json.RawMessage, updatedAt time.Time) error {
	if id == uuid.Nil || !json.Valid(resultJSON) || updatedAt.IsZero() {
		return ErrInvalid
	}
	result := r.db.WithContext(ctx).Model(&model.Task{}).Where("id = ?", model.BinaryUUIDFrom(id)).Updates(map[string]any{"status": model.TaskSucceeded, "result": resultJSON, "updated_at": updatedAt.UTC()})
	if result.Error != nil {
		return mapError(result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) DeleteTask(ctx context.Context, id uuid.UUID) error {
	result := r.db.WithContext(ctx).Where("id = ?", model.BinaryUUIDFrom(id)).Delete(&model.Task{})
	if result.Error != nil {
		return mapError(result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func validProjectStatus(status model.ProjectStatus) bool {
	return status == model.ProjectActive || status == model.ProjectDisabled
}

func validateTask(task model.Task) error {
	validStatus := task.Status == model.TaskPending || task.Status == model.TaskRunning || task.Status == model.TaskSucceeded || task.Status == model.TaskFailed || task.Status == model.TaskCanceled
	if task.ID.IsZero() || task.ProjectID.IsZero() || !validStatus || task.AvailableAt.IsZero() || task.CreatedAt.IsZero() || task.UpdatedAt.IsZero() || !json.Valid(task.Payload) || task.AttemptNo < 0 {
		return ErrInvalid
	}
	if (task.IdempotencyKey == nil) != (task.RequestHash == nil) || (task.RequestHash != nil && len(task.RequestHash) != 32) {
		return ErrInvalid
	}
	if task.IdempotencyKey != nil && (*task.IdempotencyKey == "" || len(*task.IdempotencyKey) > 128) {
		return ErrInvalid
	}
	return nil
}
