//go:build integration

package integration_test

// Phase 2 integration cases share the PostgreSQL container and migration
// fixture in this package. Concrete fetch/lease/result race assertions are
// added at their reference checkpoints.
