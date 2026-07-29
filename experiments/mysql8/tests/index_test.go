package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/database"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/dataset"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/migration"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/model"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/pagination"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/queryplan"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/testkit"
)

func TestExplainAnalyzeCursorPagination(t *testing.T) {
	environment := testkit.StartMySQL(t)
	runner, err := migration.New(environment.MigrationDSN, environment.MigrationsPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Up(); err != nil {
		t.Fatal(err)
	}
	defer runner.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	db, err := database.Open(ctx, environment.Config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	summary, err := dataset.Seed(ctx, db.SQL, 42, dataset.DefaultTaskCount)
	if err != nil {
		t.Fatal(err)
	}
	var mysqlVersion string
	if err := db.SQL.QueryRowContext(ctx, `SELECT VERSION()`).Scan(&mysqlVersion); err != nil {
		t.Fatal(err)
	}
	pager, err := pagination.New(db.SQL)
	if err != nil {
		t.Fatal(err)
	}
	offsetPage, err := pager.Offset(ctx, summary.DeepProjectID, summary.DeepStatus, 50, 50_000)
	if err != nil || len(offsetPage) != 50 {
		t.Fatalf("offset page len=%d err=%v", len(offsetPage), err)
	}
	anchorPage, err := pager.Offset(ctx, summary.DeepProjectID, summary.DeepStatus, 1, 49_999)
	if err != nil || len(anchorPage) != 1 {
		t.Fatalf("anchor len=%d err=%v", len(anchorPage), err)
	}
	after := pagination.Cursor{CreatedAt: anchorPage[0].CreatedAt, ID: anchorPage[0].ID}
	cursorPage, err := pager.Cursor(ctx, summary.DeepProjectID, summary.DeepStatus, 50, &after)
	if err != nil || len(cursorPage) != 50 {
		t.Fatalf("cursor page len=%d err=%v", len(cursorPage), err)
	}
	for i := range offsetPage {
		if offsetPage[i].ID != cursorPage[i].ID {
			t.Fatalf("page mismatch at %d: offset=%s cursor=%s", i, offsetPage[i].ID, cursorPage[i].ID)
		}
	}

	scenarios := []struct {
		name  string
		index string
	}{
		{name: "no_suitable_index"},
		{name: "single_column", index: `CREATE INDEX idx_experiment ON lab_tasks(status)`},
		{name: "wrong_composite_order", index: `CREATE INDEX idx_experiment ON lab_tasks(status,created_at DESC,project_id,id DESC)`},
		{name: "correct_composite", index: `CREATE INDEX idx_experiment ON lab_tasks(project_id,status,created_at DESC,id DESC)`},
		{name: "covering", index: `CREATE INDEX idx_experiment ON lab_tasks(project_id,status,created_at DESC,id DESC,priority)`},
	}
	for _, scenario := range scenarios {
		if _, err := db.SQL.ExecContext(ctx, `DROP INDEX idx_lab_task_page ON lab_tasks`); err != nil && !strings.Contains(err.Error(), "check that column/key exists") {
			t.Fatal(err)
		}
		if _, err := db.SQL.ExecContext(ctx, `DROP INDEX idx_lab_task_page_cover ON lab_tasks`); err != nil && !strings.Contains(err.Error(), "check that column/key exists") {
			t.Fatal(err)
		}
		_, _ = db.SQL.ExecContext(ctx, `DROP INDEX idx_experiment ON lab_tasks`)
		if scenario.index != "" {
			if _, err := db.SQL.ExecContext(ctx, scenario.index); err != nil {
				t.Fatal(err)
			}
		}
		offsetPlan, err := queryplan.ExplainAnalyze(ctx, db.SQL, pagination.OffsetSQL, model.UUIDToBytes(summary.DeepProjectID), summary.DeepStatus, 50, 50_000)
		if err != nil {
			t.Fatal(err)
		}
		cursorPlan, err := queryplan.ExplainAnalyze(ctx, db.SQL, pagination.CursorSQL, model.UUIDToBytes(summary.DeepProjectID), summary.DeepStatus, after.CreatedAt, after.CreatedAt, model.UUIDToBytes(after.ID), 50)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("MYSQL8_PLAN scenario=%s rows=%d version=%s\nOFFSET:\n%s\nCURSOR:\n%s", scenario.name, summary.TaskCount, mysqlVersion, offsetPlan, cursorPlan)
	}
	if _, err := db.SQL.ExecContext(ctx, `DROP INDEX idx_experiment ON lab_tasks`); err != nil {
		t.Fatal(err)
	}
}
