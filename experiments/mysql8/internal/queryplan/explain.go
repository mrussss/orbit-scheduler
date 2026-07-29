package queryplan

import (
	"context"
	"database/sql"
	"strings"
)

func ExplainAnalyze(ctx context.Context, db *sql.DB, query string, args ...any) (string, error) {
	rows, err := db.QueryContext(ctx, "EXPLAIN ANALYZE "+query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", err
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n"), rows.Err()
}
