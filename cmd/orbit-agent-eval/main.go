package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	evalgateway "github.com/mrussss/orbit-scheduler/internal/eval/gateway"
	"github.com/mrussss/orbit-scheduler/internal/executor/agent"
	"github.com/mrussss/orbit-scheduler/internal/executor/llm"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	fixtures := flag.String("fixtures", "evals/gateway", "Gateway eval fixture directory")
	repository := flag.String("repository", "../gateway-system", "absolute or relative Gateway snapshot path")
	fake := flag.Bool("fake-provider", true, "use deterministic CI provider")
	flag.Parse()
	root, err := filepath.Abs(*repository)
	if err != nil {
		return err
	}
	cases, err := evalgateway.LoadCases(*fixtures)
	if err != nil {
		return err
	}
	for _, item := range cases {
		if item.Input.RepositoryRef != cases[0].Input.RepositoryRef {
			return errors.New("Gateway eval cases do not share one repository_ref")
		}
	}
	if err := evalgateway.VerifyRepositoryRef(root, cases[0].Input.RepositoryRef); err != nil {
		return err
	}
	toolbox, err := agent.NewToolbox(map[string]string{"gateway": root}, agent.ToolLimits{MaxFileBytes: 256 << 10, MaxResultBytes: 128 << 10, MaxMatches: 100})
	if err != nil {
		return err
	}
	var shared *llm.OpenAICompatible
	if !*fake {
		shared, err = llm.NewOpenAICompatible(llm.OpenAICompatibleConfig{BaseURL: os.Getenv("LLM_BASE_URL"), APIKey: os.Getenv("LLM_API_KEY"), RequestTimeout: 45 * time.Second, DialTimeout: 5 * time.Second, TLSHandshakeTimeout: 5 * time.Second, MaxResponseBytes: 1 << 20})
		if err != nil {
			return err
		}
		defer shared.CloseIdleConnections()
	}
	model := strings.TrimSpace(os.Getenv("AGENT_MODEL"))
	if model == "" {
		model = "fake-gateway-agent"
	}
	encoder := json.NewEncoder(os.Stdout)
	allSucceeded := true
	for _, item := range cases {
		var provider llm.ToolProvider = shared
		if *fake {
			provider = evalgateway.NewFakeProvider(item.Expected)
		}
		executor, err := agent.NewExecutor(agent.ExecutorConfig{Provider: provider, Toolbox: toolbox, Repositories: map[string]string{"gateway": root}, Model: model, MaxIssueBytes: 64 << 10, MaxOutputTokens: 4096, MaxModelSteps: 4, MaxToolCalls: 12, MaxConcurrency: 1})
		if err != nil {
			return err
		}
		payload, _ := json.Marshal(agent.Payload{RepositoryRoot: item.Input.RepositoryRoot, Issue: item.Input.Issue, ErrorLog: item.Input.ErrorLog + "\n" + item.FaultLog})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		result := executor.Execute(ctx, scheduler.Assignment{TaskID: uuid.New(), ProjectID: uuid.New(), TaskType: "agent", Payload: payload, AttemptNo: 1})
		cancel()
		if len(result.Result) == 0 {
			allSucceeded = false
			_ = encoder.Encode(map[string]any{"case_id": item.ID, "success": false, "error": result.ErrorMessage})
			continue
		}
		var contract agent.ResultContract
		if err := json.Unmarshal(result.Result, &contract); err != nil {
			return err
		}
		metrics := evalgateway.Score(item, contract)
		allSucceeded = allSucceeded && metrics.Success
		if err := encoder.Encode(metrics); err != nil {
			return err
		}
	}
	if !allSucceeded {
		return errors.New("one or more Gateway eval cases failed")
	}
	return nil
}
