package config

import (
	"strings"
	"testing"
)

func TestCheckProdOnlyBlocksProduction(t *testing.T) {
	unsafe := CommonProdProblems(
		Postgres{MigrateOnStart: true},
		Kafka{AutoCreateTopics: true},
		&GRPCServer{Reflection: true},
	)
	if err := CheckProd(Base{Env: "local"}, unsafe...); err != nil {
		t.Fatalf("local must never be blocked: %v", err)
	}
	err := CheckProd(Base{Env: "prod"}, unsafe...)
	if err == nil {
		t.Fatal("prod with unsafe settings must fail")
	}
	for _, want := range []string{"POSTGRES_MIGRATE_ON_START", "KAFKA_AUTO_CREATE_TOPICS", "GRPC_REFLECTION"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestCheckProdSafeSettingsPass(t *testing.T) {
	safe := CommonProdProblems(Postgres{}, Kafka{}, &GRPCServer{})
	if err := CheckProd(Base{Env: "prod"}, safe...); err != nil {
		t.Fatalf("safe prod config rejected: %v", err)
	}
}

func TestMigrationJobIsAllowedInProd(t *testing.T) {
	// The migration Job keeps MigrateOnStart at its default but sets MIGRATE_ONLY: it exits after migrating.
	p := CommonProdProblems(Postgres{MigrateOnStart: true, MigrateOnly: true}, Kafka{}, nil)
	if err := CheckProd(Base{Env: "prod"}, p...); err != nil {
		t.Fatalf("migration job rejected: %v", err)
	}
}
