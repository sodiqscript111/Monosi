package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type WorkloadReference struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Name       string `json:"name"`
	Namespace  string `json:"namespace,omitempty"`
}

type ShuffleShardAssignmentSpec struct {
	TenantID        string             `json:"tenantID"`
	ShardPoolSize   int                `json:"shardPoolSize"`
	ShardsPerTenant int                `json:"shardsPerTenant"`
	NodeSelectorKey string             `json:"nodeSelectorKey,omitempty"`
	TargetWorkload  *WorkloadReference `json:"targetWorkload,omitempty"`
}

type ShuffleShardAssignmentStatus struct {
	AssignedShards     []int              `json:"assignedShards,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	LastReconciled     *metav1.Time       `json:"lastReconciled,omitempty"`
}

type ShuffleShardAssignment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ShuffleShardAssignmentSpec   `json:"spec,omitempty"`
	Status ShuffleShardAssignmentStatus `json:"status,omitempty"`
}

type ShuffleShardAssignmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ShuffleShardAssignment `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ShuffleShardAssignment{}, &ShuffleShardAssignmentList{})
}
