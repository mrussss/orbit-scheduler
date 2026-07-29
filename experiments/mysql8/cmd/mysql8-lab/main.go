package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/config"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/migration"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("mysql8-lab", flag.ContinueOnError)
	migrationsPath := flags.String("migrations", "../../migrations/mysql8", "path to MySQL lab migrations")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return fmt.Errorf("usage: mysql8-lab [-migrations path] up|down|version")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	migrationDSN, err := cfg.MigrationDSN()
	if err != nil {
		return err
	}
	runner, err := migration.New(migrationDSN, *migrationsPath)
	if err != nil {
		return err
	}
	defer runner.Close()
	switch flags.Arg(0) {
	case "up":
		changed, err := runner.Up()
		fmt.Printf("changed=%t\n", changed)
		return err
	case "down":
		changed, err := runner.Down()
		fmt.Printf("changed=%t\n", changed)
		return err
	case "version":
		version, dirty, err := runner.Version()
		if err == nil {
			fmt.Printf("version=%d dirty=%t\n", version, dirty)
		}
		return err
	default:
		return fmt.Errorf("unknown command %q", flags.Arg(0))
	}
}
