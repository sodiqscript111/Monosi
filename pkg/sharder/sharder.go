package sharder

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

var (
	ErrInvalidTenantID        = errors.New("tenantID must not be empty")
	ErrInvalidPoolSize        = errors.New("shardPoolSize (N) must be at least 1")
	ErrInvalidShardsPerTenant = errors.New("shardsPerTenant (k) must be between 1 and shardPoolSize (N)")
)

type SplitMix64 struct {
	state uint64
}

func NewSplitMix64(seed uint64) *SplitMix64 {
	return &SplitMix64{state: seed}
}

func (s *SplitMix64) NextUint64() uint64 {
	s.state += 0x9e3779b97f4a7c15
	z := s.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (s *SplitMix64) NextInt(bound int) int {
	if bound <= 1 {
		return 0
	}
	threshold := -uint64(bound) % uint64(bound)
	for {
		r := s.NextUint64()
		if r >= threshold {
			return int(r % uint64(bound))
		}
	}
}

func ValidateParams(tenantID string, n, k int) error {
	if tenantID == "" {
		return ErrInvalidTenantID
	}
	if n < 1 {
		return ErrInvalidPoolSize
	}
	if k < 1 || k > n {
		return ErrInvalidShardsPerTenant
	}
	return nil
}

func SeedFromTenant(tenantID string) uint64 {
	h := sha256.New()
	h.Write([]byte("monosi.io/shuffle-sharding/v1alpha1:"))
	h.Write([]byte(tenantID))
	digest := h.Sum(nil)
	return binary.BigEndian.Uint64(digest[:8])
}

func ComputeAssignment(tenantID string, n, k int) ([]int, error) {
	if err := ValidateParams(tenantID, n, k); err != nil {
		return nil, err
	}

	seed := SeedFromTenant(tenantID)
	rng := NewSplitMix64(seed)

	pool := make([]int, n)
	for i := 0; i < n; i++ {
		pool[i] = i
	}

	for i := 0; i < k; i++ {
		j := i + rng.NextInt(n-i)
		pool[i], pool[j] = pool[j], pool[i]
	}

	result := make([]int, k)
	copy(result, pool[:k])
	sort.Ints(result)

	return result, nil
}

func BinomialCoefficient(n, k int) (int64, error) {
	if n < 0 || k < 0 || k > n {
		return 0, fmt.Errorf("invalid binomial arguments: n=%d, k=%d", n, k)
	}
	if k == 0 || k == n {
		return 1, nil
	}
	if k > n/2 {
		k = n - k
	}
	res := int64(1)
	for i := 1; i <= k; i++ {
		res = res * int64(n-i+1) / int64(i)
	}
	return res, nil
}

func BlastRadius(n, k int) float64 {
	if n <= 0 || k <= 0 {
		return 0.0
	}
	return float64(k) / float64(n)
}

func Overlap(a, b []int) int {
	count := 0
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			count++
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return count
}
