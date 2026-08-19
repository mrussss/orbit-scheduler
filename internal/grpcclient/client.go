package grpcclient

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	workerv1 "github.com/mrussss/orbit-scheduler/gen/orbit/worker/v1"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
	workerpkg "github.com/mrussss/orbit-scheduler/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Client struct {
	connection *grpc.ClientConn
	rpc        workerv1.WorkerServiceClient
}

func Dial(ctx context.Context, address string) (*Client, error) {
	if strings.HasPrefix(address, ":") {
		address = "localhost" + address
	}
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(domain.MaxGRPCMessageBytes), grpc.MaxCallSendMsgSize(domain.MaxGRPCMessageBytes)))
	if err != nil {
		return nil, err
	}
	return &Client{connection: connection, rpc: workerv1.NewWorkerServiceClient(connection)}, nil
}
func (c *Client) Close() error { return c.connection.Close() }
func (c *Client) Register(ctx context.Context, r workerpkg.Registration) error {
	_, err := c.rpc.RegisterWorker(ctx, &workerv1.RegisterWorkerRequest{WorkerName: r.WorkerName, WorkerInstanceId: r.InstanceID.String(), Hostname: r.Hostname, Capacity: int32(r.Capacity), SupportedTaskTypes: r.TaskTypes, ProcessVersion: r.ProcessVersion, MetadataJson: r.Metadata})
	return classify(err)
}
func (c *Client) Heartbeat(ctx context.Context, h workerpkg.Heartbeat) error {
	_, err := c.rpc.Heartbeat(ctx, &workerv1.HeartbeatRequest{WorkerInstanceId: h.InstanceID.String(), RunningTasks: int32(h.Running), AvailableCapacity: int32(h.Available), Draining: h.Draining, ProcessUptimeMs: h.Uptime.Milliseconds()})
	return classify(err)
}
func (c *Client) Fetch(ctx context.Context, request scheduler.FetchRequest) ([]scheduler.Assignment, error) {
	response, err := c.rpc.FetchTasks(ctx, &workerv1.FetchTasksRequest{WorkerInstanceId: request.WorkerInstanceID.String(), Requested: int32(request.Requested), LeaseDurationMs: request.LeaseDuration.Milliseconds(), TraceId: request.TraceID})
	if err != nil {
		return nil, classify(err)
	}
	tasks := make([]scheduler.Assignment, len(response.GetTasks()))
	for i, item := range response.GetTasks() {
		taskID, err := uuid.Parse(item.GetTaskId())
		if err != nil {
			return nil, err
		}
		projectID, err := uuid.Parse(item.GetProjectId())
		if err != nil {
			return nil, err
		}
		task := scheduler.Assignment{TaskID: taskID, ProjectID: projectID, TaskType: item.GetTaskType(), Payload: item.GetPayloadJson(), AttemptNo: int(item.GetAttemptNo()), LeaseExpiresAt: time.UnixMilli(item.GetLeaseExpiresAtUnixMs()), ExecutionTimeout: time.Duration(item.GetExecutionTimeoutMs()) * time.Millisecond, TraceID: item.GetTraceId()}
		if item.JobId != nil {
			id, err := uuid.Parse(item.GetJobId())
			if err != nil {
				return nil, err
			}
			task.JobID = &id
		}
		if item.OverallDeadlineUnixMs != nil {
			deadline := time.UnixMilli(item.GetOverallDeadlineUnixMs())
			task.OverallDeadline = &deadline
		}
		tasks[i] = task
	}
	return tasks, nil
}
func (c *Client) Renew(ctx context.Context, request scheduler.RenewRequest) (scheduler.RenewResult, error) {
	response, err := c.rpc.RenewLease(ctx, &workerv1.RenewLeaseRequest{TaskId: request.TaskID.String(), WorkerInstanceId: request.WorkerInstanceID.String(), AttemptNo: int32(request.AttemptNo), ExtensionMs: request.Extension.Milliseconds()})
	if err != nil {
		return scheduler.RenewResult{}, classify(err)
	}
	return scheduler.RenewResult{LeaseExpiresAt: time.UnixMilli(response.GetLeaseExpiresAtUnixMs()), CancelRequested: response.GetCancelRequested()}, nil
}
func (c *Client) Report(ctx context.Context, request scheduler.ReportRequest) (scheduler.ReportResult, error) {
	response, err := c.rpc.ReportResult(ctx, &workerv1.ReportResultRequest{TaskId: request.TaskID.String(), WorkerInstanceId: request.WorkerInstanceID.String(), AttemptNo: int32(request.AttemptNo), Outcome: outcomeProto(request.Outcome), ResultJson: request.Result, ResultHash: request.ResultHash[:], ErrorType: string(request.ErrorType), ErrorMessage: request.ErrorMessage, ExecutionStartedAtUnixMs: request.ExecutionStartedAt.UnixMilli(), ExecutionFinishedAtUnixMs: request.ExecutionFinishedAt.UnixMilli()})
	if err != nil {
		return scheduler.ReportResult{}, classify(err)
	}
	result := scheduler.ReportResult{Status: domain.TaskStatus(response.GetStatus()), Idempotent: response.GetIdempotent()}
	if response.AvailableAtUnixMs != nil {
		value := time.UnixMilli(response.GetAvailableAtUnixMs())
		result.AvailableAt = &value
	}
	return result, nil
}
func (c *Client) SetDraining(ctx context.Context, id uuid.UUID, draining bool) error {
	_, err := c.rpc.SetDraining(ctx, &workerv1.SetDrainingRequest{WorkerInstanceId: id.String(), Draining: draining})
	return classify(err)
}

func (c *Client) RecordAgentStep(ctx context.Context, request scheduler.RecordAgentStepRequest) error {
	wire := &workerv1.RecordAgentStepRequest{TaskId: request.TaskID.String(), WorkerInstanceId: request.WorkerInstanceID.String(), AttemptNo: int32(request.AttemptNo), StepNo: int32(request.StepNo), Kind: string(request.Kind), ToolName: request.ToolName, InputSummaryJson: request.InputSummary, OutputSummaryJson: request.OutputSummary, Status: string(request.Status), StartedAtUnixMs: request.StartedAt.UnixMilli()}
	if request.FinishedAt != nil {
		finished := request.FinishedAt.UnixMilli()
		wire.FinishedAtUnixMs = &finished
	}
	_, err := c.rpc.RecordAgentStep(ctx, wire)
	return classify(err)
}
func outcomeProto(value domain.TaskOutcome) workerv1.TaskOutcome {
	mapping := map[domain.TaskOutcome]workerv1.TaskOutcome{domain.OutcomeSucceeded: workerv1.TaskOutcome_TASK_OUTCOME_SUCCEEDED, domain.OutcomeRetryableFailure: workerv1.TaskOutcome_TASK_OUTCOME_RETRYABLE_FAILURE, domain.OutcomePermanentFailure: workerv1.TaskOutcome_TASK_OUTCOME_PERMANENT_FAILURE, domain.OutcomeTimeout: workerv1.TaskOutcome_TASK_OUTCOME_TIMEOUT, domain.OutcomeCanceled: workerv1.TaskOutcome_TASK_OUTCOME_CANCELED}
	return mapping[value]
}
func classify(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.NotFound:
		return scheduler.ErrNotFound
	case codes.FailedPrecondition:
		return scheduler.ErrStaleLease
	case codes.AlreadyExists:
		return scheduler.ErrConflict
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.Canceled:
		return context.Canceled
	default:
		return errors.New(status.Convert(err).Message())
	}
}
