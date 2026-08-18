package observability

import (
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

func RegisterDatabaseMetrics(registerer prometheus.Registerer, gormPool *sql.DB, pgxPool *pgxpool.Pool) error {
	if registerer == nil || gormPool == nil || pgxPool == nil {
		return errors.New("database metrics dependencies are required")
	}
	gauges := []prometheus.Collector{
		poolGauge("gorm", "open", func() float64 { return float64(gormPool.Stats().OpenConnections) }),
		poolGauge("gorm", "in_use", func() float64 { return float64(gormPool.Stats().InUse) }),
		poolGauge("gorm", "idle", func() float64 { return float64(gormPool.Stats().Idle) }),
		poolGauge("gorm", "max", func() float64 { return float64(gormPool.Stats().MaxOpenConnections) }),
		poolGauge("pgx", "open", func() float64 { return float64(pgxPool.Stat().TotalConns()) }),
		poolGauge("pgx", "in_use", func() float64 { return float64(pgxPool.Stat().AcquiredConns()) }),
		poolGauge("pgx", "idle", func() float64 { return float64(pgxPool.Stat().IdleConns()) }),
		poolGauge("pgx", "max", func() float64 { return float64(pgxPool.Config().MaxConns) }),
	}
	for _, gauge := range gauges {
		if err := registerer.Register(gauge); err != nil {
			return err
		}
	}
	return nil
}

func poolGauge(pool, state string, value func() float64) prometheus.GaugeFunc {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "orbit_database_pool_connections",
		Help:        "Database connection pool state by bounded pool and state labels.",
		ConstLabels: prometheus.Labels{"pool": pool, "state": state},
	}, value)
}
