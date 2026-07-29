package config

import (
	"strings"
	"testing"
	"time"
)

func TestConfigSeparatesRuntimeAndMigrationDSNs(t *testing.T) {
	valid := Config{DSN: "orbit:orbit@tcp(localhost:3306)/orbit_lab?parseTime=true&loc=UTC", MaxOpenConns: 4, MaxIdleConns: 2, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute, TxTimeout: time.Second}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	migrationDSN, err := valid.MigrationDSN()
	if err != nil || !strings.Contains(migrationDSN, "multiStatements=true") {
		t.Fatalf("migration DSN=%q err=%v", migrationDSN, err)
	}
	invalid := valid
	invalid.DSN = "orbit:orbit@tcp(localhost:3306)/orbit_lab"
	err = invalid.Validate()
	if err == nil || !strings.Contains(err.Error(), "parseTime=true") || !strings.Contains(err.Error(), "loc=UTC") {
		t.Fatalf("unexpected validation error: %v", err)
	}
	unsafe := valid
	unsafe.DSN += "&multiStatements=true"
	if err := unsafe.Validate(); err == nil || !strings.Contains(err.Error(), "must not enable multiStatements") {
		t.Fatalf("unsafe runtime DSN error=%v", err)
	}
}
