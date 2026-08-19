package grpcservice

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	workerv1 "github.com/mrussss/orbit-scheduler/gen/orbit/worker/v1"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxCapacity     = 1024
	maxMessageBytes = 1 << 20
)

type Store interface {
	scheduler.Store
	RegisterWorker(context.Context, domain.WorkerInstance) error
	HeartbeatWorker(context.Context, uuid.UUID, int, bool) error
	SetWorkerDraining(context.Context, uuid.UUID, bool) error
	WorkerServerTime(context.Context) (time.Time, error)
	RecordAgentStep(context.Context, scheduler.RecordAgentStepRequest) error
}
type Service struct {
	workerv1.UnimplementedWorkerServiceServer
	store   Store
	metrics Metrics
}

type Metrics interface {
	Fetch(outcome string, count int, duration time.Duration)
	Renew(outcome string, duration time.Duration)
	Report(outcome string, duration time.Duration)
}

type noopMetrics struct{}

func (noopMetrics) Fetch(string, int, time.Duration) {}
func (noopMetrics) Renew(string, time.Duration)      {}
func (noopMetrics) Report(string, time.Duration)     {}

func New(store Store, observers ...Metrics) (*Service, error) {
	if store == nil {
		return nil, errors.New("grpc worker store is required")
	}
	metrics := Metrics(noopMetrics{})
	if len(observers) > 0 && observers[0] != nil {
		metrics = observers[0]
	}
	return &Service{store: store, metrics: metrics}, nil
}
func (s *Service) RegisterWorker(ctx context.Context, req *workerv1.RegisterWorkerRequest) (*workerv1.RegisterWorkerResponse, error) {
	id, err := uuid.Parse(req.GetWorkerInstanceId())
	if err != nil || req.GetWorkerName() == "" || req.GetHostname() == "" || req.GetCapacity() <= 0 || req.GetCapacity() > maxCapacity || len(req.GetSupportedTaskTypes()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid worker registration")
	}
	for _, taskType := range req.GetSupportedTaskTypes() {
		if taskType == "" {
			return nil, status.Error(codes.InvalidArgument, "empty supported task type")
		}
	}
	if len(req.GetMetadataJson()) > maxMessageBytes {
		return nil, status.Error(codes.ResourceExhausted, "metadata too large")
	}
	worker := domain.WorkerInstance{ID: id, WorkerName: req.GetWorkerName(), Hostname: req.GetHostname(), Capacity: int(req.GetCapacity()), SupportedTaskTypes: req.GetSupportedTaskTypes(), ProcessVersion: req.GetProcessVersion(), Metadata: req.GetMetadataJson()}
	if err := s.store.RegisterWorker(ctx, worker); err != nil {
		return nil, mapError(err)
	}
	now, err := s.store.WorkerServerTime(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	return &workerv1.RegisterWorkerResponse{RegisteredAtUnixMs: now.UnixMilli()}, nil
}
func (s *Service) Heartbeat(ctx context.Context, req *workerv1.HeartbeatRequest) (*workerv1.HeartbeatResponse, error) {
	id, err := uuid.Parse(req.GetWorkerInstanceId())
	if err != nil || req.GetRunningTasks() < 0 || req.GetAvailableCapacity() < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid heartbeat")
	}
	if err := s.store.HeartbeatWorker(ctx, id, int(req.GetRunningTasks()), req.GetDraining()); err != nil {
		return nil, mapError(err)
	}
	now, err := s.store.WorkerServerTime(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	return &workerv1.HeartbeatResponse{ServerTimeUnixMs: now.UnixMilli()}, nil
}
func (s *Service) FetchTasks(ctx context.Context, req *workerv1.FetchTasksRequest) (*workerv1.FetchTasksResponse, error) {
	started := time.Now()
	id, err := uuid.Parse(req.GetWorkerInstanceId())
	if err != nil || req.GetRequested() <= 0 || req.GetLeaseDurationMs() <= 0 {
		s.metrics.Fetch("invalid_argument", 0, time.Since(started))
		return nil, status.Error(codes.InvalidArgument, "invalid fetch request")
	}
	tasks, err := s.store.FetchTasks(ctx, scheduler.FetchRequest{WorkerInstanceID: id, Requested: int(req.GetRequested()), LeaseDuration: time.Duration(req.GetLeaseDurationMs()) * time.Millisecond, TraceID: req.GetTraceId()})
	if err != nil {
		s.metrics.Fetch(metricOutcome(err), 0, time.Since(started))
		return nil, mapError(err)
	}
	s.metrics.Fetch("success", len(tasks), time.Since(started))
	response := &workerv1.FetchTasksResponse{Tasks: make([]*workerv1.TaskAssignment, len(tasks))}
	for i, task := range tasks {
		item := &workerv1.TaskAssignment{TaskId: task.TaskID.String(), ProjectId: task.ProjectID.String(), TaskType: task.TaskType, PayloadJson: task.Payload, AttemptNo: int32(task.AttemptNo), LeaseExpiresAtUnixMs: task.LeaseExpiresAt.UnixMilli(), ExecutionTimeoutMs: task.ExecutionTimeout.Milliseconds(), TraceId: task.TraceID}
		if task.JobID != nil {
			value := task.JobID.String()
			item.JobId = &value
		}
		if task.OverallDeadline != nil {
			value := task.OverallDeadline.UnixMilli()
			item.OverallDeadlineUnixMs = &value
		}
		response.Tasks[i] = item
	}
	return response, nil
}
func (s *Service) RenewLease(ctx context.Context, req *workerv1.RenewLeaseRequest) (*workerv1.RenewLeaseResponse, error) {
	started := time.Now()
	taskID, workerID, err := parsePair(req.GetTaskId(), req.GetWorkerInstanceId())
	if err != nil || req.GetAttemptNo() <= 0 || req.GetExtensionMs() <= 0 {
		s.metrics.Renew("invalid_argument", time.Since(started))
		return nil, status.Error(codes.InvalidArgument, "invalid lease renewal")
	}
	result, err := s.store.RenewLease(ctx, scheduler.RenewRequest{TaskID: taskID, WorkerInstanceID: workerID, AttemptNo: int(req.GetAttemptNo()), Extension: time.Duration(req.GetExtensionMs()) * time.Millisecond})
	if err != nil {
		s.metrics.Renew(metricOutcome(err), time.Since(started))
		return nil, mapError(err)
	}
	s.metrics.Renew("success", time.Since(started))
	return &workerv1.RenewLeaseResponse{LeaseExpiresAtUnixMs: result.LeaseExpiresAt.UnixMilli(), CancelRequested: result.CancelRequested}, nil
}
func (s *Service) ReportResult(ctx context.Context, req *workerv1.ReportResultRequest) (*workerv1.ReportResultResponse, error) {
	started := time.Now()
	taskID, workerID, err := parsePair(req.GetTaskId(), req.GetWorkerInstanceId())
	outcome, ok := protoOutcome(req.GetOutcome())
	if err != nil || !ok || req.GetAttemptNo() <= 0 || len(req.GetResultHash()) != 32 || len(req.GetResultJson()) > domain.MaxResultBytes {
		s.metrics.Report("invalid_argument", time.Since(started))
		return nil, status.Error(codes.InvalidArgument, "invalid result")
	}
	var hash [32]byte
	copy(hash[:], req.GetResultHash())
	result, err := s.store.ReportResult(ctx, scheduler.ReportRequest{TaskID: taskID, WorkerInstanceID: workerID, AttemptNo: int(req.GetAttemptNo()), Outcome: outcome, Result: req.GetResultJson(), ResultHash: hash, ErrorType: domain.ErrorType(req.GetErrorType()), ErrorMessage: req.GetErrorMessage(), ExecutionStartedAt: time.UnixMilli(req.GetExecutionStartedAtUnixMs()), ExecutionFinishedAt: time.UnixMilli(req.GetExecutionFinishedAtUnixMs())})
	if err != nil {
		s.metrics.Report(metricOutcome(err), time.Since(started))
		return nil, mapError(err)
	}
	s.metrics.Report("success", time.Since(started))
	response := &workerv1.ReportResultResponse{Status: string(result.Status), Idempotent: result.Idempotent}
	if result.AvailableAt != nil {
		value := result.AvailableAt.UnixMilli()
		response.AvailableAtUnixMs = &value
	}
	return response, nil
}
func (s *Service) SetDraining(ctx context.Context, req *workerv1.SetDrainingRequest) (*workerv1.SetDrainingResponse, error) {
	id, err := uuid.Parse(req.GetWorkerInstanceId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid worker instance id")
	}
	if err := s.store.SetWorkerDraining(ctx, id, req.GetDraining()); err != nil {
		return nil, mapError(err)
	}
	now, err := s.store.WorkerServerTime(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	return &workerv1.SetDrainingResponse{ServerTimeUnixMs: now.UnixMilli()}, nil
}

func (s *Service) RecordAgentStep(ctx context.Context, req *workerv1.RecordAgentStepRequest) (*workerv1.RecordAgentStepResponse, error) {
	taskID, workerID, err := parsePair(req.GetTaskId(), req.GetWorkerInstanceId())
	if err != nil || req.GetAttemptNo() <= 0 || req.GetStepNo() <= 0 || len(req.GetInputSummaryJson()) > 8192 || len(req.GetOutputSummaryJson()) > 8192 || req.GetStartedAtUnixMs() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid agent step")
	}
	request := scheduler.RecordAgentStepRequest{TaskID: taskID, WorkerInstanceID: workerID, AttemptNo: int(req.GetAttemptNo()), StepNo: int(req.GetStepNo()), Kind: scheduler.AgentStepKind(req.GetKind()), ToolName: req.GetToolName(), InputSummary: req.GetInputSummaryJson(), OutputSummary: req.GetOutputSummaryJson(), Status: scheduler.AgentStepStatus(req.GetStatus()), StartedAt: time.UnixMilli(req.GetStartedAtUnixMs())}
	if req.FinishedAtUnixMs != nil {
		finished := time.UnixMilli(req.GetFinishedAtUnixMs())
		request.FinishedAt = &finished
	}
	if err := s.store.RecordAgentStep(ctx, request); err != nil {
		return nil, mapError(err)
	}
	return &workerv1.RecordAgentStepResponse{}, nil
}
func parsePair(task, worker string) (uuid.UUID, uuid.UUID, error) {
	taskID, err := uuid.Parse(task)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	workerID, err := uuid.Parse(worker)
	return taskID, workerID, err
}
func protoOutcome(value workerv1.TaskOutcome) (domain.TaskOutcome, bool) {
	mapping := map[workerv1.TaskOutcome]domain.TaskOutcome{workerv1.TaskOutcome_TASK_OUTCOME_SUCCEEDED: domain.OutcomeSucceeded, workerv1.TaskOutcome_TASK_OUTCOME_RETRYABLE_FAILURE: domain.OutcomeRetryableFailure, workerv1.TaskOutcome_TASK_OUTCOME_PERMANENT_FAILURE: domain.OutcomePermanentFailure, workerv1.TaskOutcome_TASK_OUTCOME_TIMEOUT: domain.OutcomeTimeout, workerv1.TaskOutcome_TASK_OUTCOME_CANCELED: domain.OutcomeCanceled}
	out, ok := mapping[value]
	return out, ok
}
func mapError(err error) error {
	switch {
	case errors.Is(err, scheduler.ErrNotFound), errors.Is(err, scheduler.ErrWorkerNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, scheduler.ErrStaleLease):
		return status.Error(codes.FailedPrecondition, "stale lease")
	case errors.Is(err, scheduler.ErrConflict):
		return status.Error(codes.AlreadyExists, "conflicting result")
	case errors.Is(err, scheduler.ErrWorkerDraining):
		return status.Error(codes.FailedPrecondition, "worker draining")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	default:
		return status.Error(codes.Internal, "internal server error")
	}
}

func metricOutcome(err error) string {
	switch {
	case errors.Is(err, scheduler.ErrNotFound), errors.Is(err, scheduler.ErrWorkerNotFound):
		return "not_found"
	case errors.Is(err, scheduler.ErrStaleLease):
		return "stale_lease"
	case errors.Is(err, scheduler.ErrConflict):
		return "conflict"
	case errors.Is(err, scheduler.ErrWorkerDraining):
		return "worker_draining"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "error"
	}
}
