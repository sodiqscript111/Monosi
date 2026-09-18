package sharder

import (
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestStabilityAcrossCalls(t *testing.T) {
	tenants := []string{
		"tenant-alpha",
		"tenant-beta",
		"tenant-gamma",
		"acme-corp-prod",
		"tenant-9999",
		"random-tenant-xyz",
	}

	n := 8
	k := 2

	for _, tenant := range tenants {
		t.Run(tenant, func(t *testing.T) {
			initial, err := ComputeAssignment(tenant, n, k)
			if err != nil {
				t.Fatalf("failed to compute initial assignment: %v", err)
			}
			if len(initial) != k {
				t.Fatalf("expected %d shards, got %d", k, len(initial))
			}

			for i := 0; i < 500; i++ {
				subsequent, err := ComputeAssignment(tenant, n, k)
				if err != nil {
					t.Fatalf("failed on iteration %d: %v", i, err)
				}
				if !reflect.DeepEqual(initial, subsequent) {
					t.Fatalf("stability failure on iter %d: expected %v, got %v", i, initial, subsequent)
				}
			}
		})
	}
}

func TestEvenDistribution(t *testing.T) {
	n := 8
	k := 2
	numTenants := 20000

	shardCounts := make(map[int]int)

	for i := 0; i < numTenants; i++ {
		tenantID := fmt.Sprintf("tenant-%d-sample-%x", i, i*7919)
		shards, err := ComputeAssignment(tenantID, n, k)
		if err != nil {
			t.Fatalf("ComputeAssignment error: %v", err)
		}

		for _, s := range shards {
			shardCounts[s]++
		}
	}

	expectedPerShard := float64(numTenants) * (float64(k) / float64(n))

	t.Logf("Expected count per shard: %.0f", expectedPerShard)

	maxAllowedDeviation := expectedPerShard * 0.03

	for shard := 0; shard < n; shard++ {
		count := shardCounts[shard]
		dev := math.Abs(float64(count) - expectedPerShard)
		pct := (dev / expectedPerShard) * 100.0
		t.Logf("Shard %d: %d occurrences (deviation: %.2f%%)", shard, count, pct)

		if dev > maxAllowedDeviation {
			t.Errorf("Shard %d count %d deviates too far from expected %.0f (allowed: %.0f, actual dev: %.0f)",
				shard, count, expectedPerShard, maxAllowedDeviation, dev)
		}
	}
}

func TestLowPairwiseOverlap(t *testing.T) {
	n := 8
	k := 2
	numTenants := 500

	assignments := make([][]int, numTenants)
	for i := 0; i < numTenants; i++ {
		tenantID := fmt.Sprintf("tenant-overlap-test-%d", i)
		shards, err := ComputeAssignment(tenantID, n, k)
		if err != nil {
			t.Fatalf("failed computing assignment: %v", err)
		}
		assignments[i] = shards
	}

	totalPairs := 0
	overlap0 := 0
	overlap1 := 0
	overlap2Identical := 0

	for i := 0; i < numTenants; i++ {
		for j := i + 1; j < numTenants; j++ {
			totalPairs++
			shared := Overlap(assignments[i], assignments[j])

			if reflect.DeepEqual(assignments[i], assignments[j]) {
				if shared != 2 {
					t.Errorf("expected overlap 2 for identical combinations, got %d", shared)
				}
				overlap2Identical++
			} else {
				if shared > 1 {
					t.Errorf("VIOLATION: distinct assignments %v and %v share %d shards (expected <= 1)",
						assignments[i], assignments[j], shared)
				}
				if shared == 1 {
					overlap1++
				} else if shared == 0 {
					overlap0++
				}
			}
		}
	}

	t.Logf("Total pairs: %d | Overlap 0: %d (%.2f%%) | Overlap 1: %d (%.2f%%) | Identical (Overlap 2): %d (%.2f%%)",
		totalPairs, overlap0, (float64(overlap0)/float64(totalPairs))*100,
		overlap1, (float64(overlap1)/float64(totalPairs))*100,
		overlap2Identical, (float64(overlap2Identical)/float64(totalPairs))*100)

	collisionPct := (float64(overlap2Identical) / float64(totalPairs)) * 100
	if math.Abs(collisionPct-3.57) > 1.5 {
		t.Errorf("Collision percentage %.2f%% deviated too much from theoretical 3.57%%", collisionPct)
	}
}

func TestValidationAndBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		tenantID  string
		n         int
		k         int
		expectErr bool
	}{
		{"valid standard", "tenant-1", 8, 2, false},
		{"valid full pool", "tenant-2", 4, 4, false},
		{"valid single shard", "tenant-3", 10, 1, false},
		{"empty tenant ID", "", 8, 2, true},
		{"zero pool size", "tenant-4", 0, 2, true},
		{"negative pool size", "tenant-5", -5, 2, true},
		{"zero shards per tenant", "tenant-6", 8, 0, true},
		{"k greater than n", "tenant-7", 4, 5, true},
		{"negative k", "tenant-8", 8, -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := ComputeAssignment(tt.tenantID, tt.n, tt.k)
			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error for %s, got nil result %v", tt.name, res)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for %s: %v", tt.name, err)
				}
				if len(res) != tt.k {
					t.Errorf("expected %d shards, got %d", tt.k, len(res))
				}
			}
		})
	}
}

func TestMathHelpers(t *testing.T) {
	c, err := BinomialCoefficient(8, 2)
	if err != nil || c != 28 {
		t.Fatalf("expected C(8, 2) = 28, got %d (err: %v)", c, err)
	}

	radius := BlastRadius(8, 2)
	if radius != 0.25 {
		t.Fatalf("expected blast radius 0.25, got %f", radius)
	}

	a := []int{0, 2, 5}
	b := []int{2, 3, 5}
	if Overlap(a, b) != 2 {
		t.Fatalf("expected overlap 2, got %d", Overlap(a, b))
	}
	cDisjoint := []int{1, 4, 7}
	if Overlap(a, cDisjoint) != 0 {
		t.Fatalf("expected overlap 0, got %d", Overlap(a, cDisjoint))
	}
}
