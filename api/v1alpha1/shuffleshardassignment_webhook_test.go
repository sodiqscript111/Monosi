package v1alpha1

import (
	"context"
	"testing"
)

func TestValidateShuffleShardAssignment(t *testing.T) {
	validator := &ShuffleShardAssignmentCustomValidator{}
	ctx := context.Background()

	valid := &ShuffleShardAssignment{
		Spec: ShuffleShardAssignmentSpec{
			TenantID:        "tenant-1",
			ShardPoolSize:   8,
			ShardsPerTenant: 2,
		},
	}

	if _, err := validator.ValidateCreate(ctx, valid); err != nil {
		t.Fatalf("expected valid assignment to pass create validation: %v", err)
	}

	if _, err := validator.ValidateUpdate(ctx, nil, valid); err != nil {
		t.Fatalf("expected valid assignment to pass update validation: %v", err)
	}

	invalidK := &ShuffleShardAssignment{
		Spec: ShuffleShardAssignmentSpec{
			TenantID:        "tenant-1",
			ShardPoolSize:   4,
			ShardsPerTenant: 6,
		},
	}

	if _, err := validator.ValidateCreate(ctx, invalidK); err == nil {
		t.Fatalf("expected create validation to fail when shardsPerTenant > shardPoolSize")
	}

	invalidEmptyTenant := &ShuffleShardAssignment{
		Spec: ShuffleShardAssignmentSpec{
			TenantID:        "",
			ShardPoolSize:   8,
			ShardsPerTenant: 2,
		},
	}

	if _, err := validator.ValidateCreate(ctx, invalidEmptyTenant); err == nil {
		t.Fatalf("expected create validation to fail when tenantID is empty")
	}

	invalidPoolZero := &ShuffleShardAssignment{
		Spec: ShuffleShardAssignmentSpec{
			TenantID:        "tenant-1",
			ShardPoolSize:   0,
			ShardsPerTenant: 2,
		},
	}

	if _, err := validator.ValidateCreate(ctx, invalidPoolZero); err == nil {
		t.Fatalf("expected create validation to fail when shardPoolSize < 1")
	}

	if _, err := validator.ValidateDelete(ctx, valid); err != nil {
		t.Fatalf("expected delete validation to pass: %v", err)
	}
}
