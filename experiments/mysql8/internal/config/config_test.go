package config

import (
	"strings"
	"testing"
	"time"
)

func TestConfigRequiresExplicitTimeAndMigrationSemantics(t *testing.T) {
	valid := Config{DSN: "orbit:orbit@tcp(localhost:3306)/orbit_lab?parseTime=true&loc=UTC&multiStatements=true", MaxOpenConns: 4, MaxIdleConns: 2, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute, TxTimeout: time.Second}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.DSN = "orbit:orbit@tcp(localhost:3306)/orbit_lab"
	err := invalid.Validate()
	if err == nil || !strings.Contains(err.Error(), "parseTime=true") || !strings.Contains(err.Error(), "loc=UTC") || !strings.Contains(err.Error(), "multiStatements=true") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}
