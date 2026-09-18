package controllers

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "monosi.io/shuffle-sharding-controller/api/v1alpha1"
)

func setupTestScheme(t *testing.T) *runtime.Scheme {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add appsv1 to scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add v1alpha1 to scheme: %v", err)
	}
	return scheme
}

func TestReconcileBasicAssignment(t *testing.T) {
	scheme := setupTestScheme(t)

	assignment := &v1alpha1.ShuffleShardAssignment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-assignment-alpha",
			Namespace: "default",
		},
		Spec: v1alpha1.ShuffleShardAssignmentSpec{
			TenantID:        "tenant-alpha",
			ShardPoolSize:   8,
			ShardsPerTenant: 2,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(assignment).
		WithStatusSubresource(assignment).
		Build()

	reconciler := &ShuffleShardAssignmentReconciler{
		Client: client,
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: "default",
			Name:      "test-assignment-alpha",
		},
	}

	ctx := context.Background()

	res, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got %v", res)
	}

	var updated v1alpha1.ShuffleShardAssignment
	if err := client.Get(ctx, req.NamespacedName, &updated); err != nil {
		t.Fatalf("failed to get updated assignment: %v", err)
	}

	if !controllerutil.ContainsFinalizer(&updated, FinalizerName) {
		t.Errorf("expected finalizer %s to be present", FinalizerName)
	}

	if len(updated.Status.AssignedShards) != 2 {
		t.Fatalf("expected 2 assigned shards, got %v", updated.Status.AssignedShards)
	}
	if updated.Status.Phase != "Assigned" {
		t.Errorf("expected Phase Assigned, got %s", updated.Status.Phase)
	}

	assignedInitial := updated.Status.AssignedShards
	lastReconciled := updated.Status.LastReconciled

	time.Sleep(10 * time.Millisecond)

	res2, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("second Reconcile failed: %v", err)
	}
	if res2.Requeue || res2.RequeueAfter != 0 {
		t.Errorf("expected no requeue on second run, got %v", res2)
	}

	var updated2 v1alpha1.ShuffleShardAssignment
	if err := client.Get(ctx, req.NamespacedName, &updated2); err != nil {
		t.Fatalf("failed to get second updated assignment: %v", err)
	}

	if len(updated2.Status.AssignedShards) != 2 ||
		updated2.Status.AssignedShards[0] != assignedInitial[0] ||
		updated2.Status.AssignedShards[1] != assignedInitial[1] {
		t.Errorf("assignment changed across reconciles: was %v, now %v",
			assignedInitial, updated2.Status.AssignedShards)
	}

	if updated2.Status.LastReconciled.Time != lastReconciled.Time {
		t.Errorf("status was unnecessarily updated without changes: was %v, now %v",
			lastReconciled.Time, updated2.Status.LastReconciled.Time)
	}
}

func TestReconcileWithTargetDeploymentAndCleanup(t *testing.T) {
	scheme := setupTestScheme(t)

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tenant-alpha-worker",
			Namespace: "default",
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "worker",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "web",
							Image: "nginx:alpine",
						},
					},
				},
			},
		},
	}

	assignment := &v1alpha1.ShuffleShardAssignment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tenant-alpha-sharding",
			Namespace: "default",
		},
		Spec: v1alpha1.ShuffleShardAssignmentSpec{
			TenantID:        "tenant-alpha",
			ShardPoolSize:   8,
			ShardsPerTenant: 2,
			NodeSelectorKey: "sharding.monosi.io/shard",
			TargetWorkload: &v1alpha1.WorkloadReference{
				Kind: "Deployment",
				Name: "tenant-alpha-worker",
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(deploy, assignment).
		WithStatusSubresource(assignment).
		Build()

	reconciler := &ShuffleShardAssignmentReconciler{
		Client: client,
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: "default",
			Name:      "tenant-alpha-sharding",
		},
	}

	ctx := context.Background()

	res, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.Requeue {
		t.Errorf("unexpected requeue: %v", res)
	}

	var updatedDeploy appsv1.Deployment
	if err := client.Get(ctx, types.NamespacedName{Namespace: "default", Name: "tenant-alpha-worker"}, &updatedDeploy); err != nil {
		t.Fatalf("failed to fetch updated deployment: %v", err)
	}

	templateLabels := updatedDeploy.Spec.Template.Labels
	if templateLabels[LabelTenant] != "tenant-alpha" {
		t.Errorf("expected tenant label 'tenant-alpha', got '%s'", templateLabels[LabelTenant])
	}
	if templateLabels[LabelSharded] != "true" {
		t.Errorf("expected sharded label 'true', got '%s'", templateLabels[LabelSharded])
	}

	var updatedCR v1alpha1.ShuffleShardAssignment
	_ = client.Get(ctx, req.NamespacedName, &updatedCR)
	assignedShards := updatedCR.Status.AssignedShards

	expectedShardStrs := make([]string, len(assignedShards))
	for i, s := range assignedShards {
		expectedShardStrs[i] = strconv.Itoa(s)
	}
	expectedAnnotation := strings.Join(expectedShardStrs, ",")

	if updatedDeploy.Spec.Template.Annotations[AnnotationShards] != expectedAnnotation {
		t.Errorf("expected annotation %s, got %s", expectedAnnotation, updatedDeploy.Spec.Template.Annotations[AnnotationShards])
	}

	affinity := updatedDeploy.Spec.Template.Spec.Affinity
	if affinity == nil || affinity.NodeAffinity == nil || affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		t.Fatalf("expected node affinity to be configured, got nil")
	}

	terms := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		t.Fatalf("expected at least 1 node selector term")
	}

	foundAffinity := false
	for _, term := range terms {
		for _, req := range term.MatchExpressions {
			if req.Key == "sharding.monosi.io/shard" && req.Operator == corev1.NodeSelectorOpIn {
				foundAffinity = true
				if len(req.Values) != len(expectedShardStrs) {
					t.Errorf("expected %d shard values in affinity, got %v", len(expectedShardStrs), req.Values)
				}
			}
		}
	}
	if !foundAffinity {
		t.Errorf("did not find expected NodeAffinity matchExpression for sharding.monosi.io/shard")
	}

	tolerations := updatedDeploy.Spec.Template.Spec.Tolerations
	if len(tolerations) < len(assignedShards) {
		t.Errorf("expected at least %d tolerations, got %d", len(assignedShards), len(tolerations))
	}
	for _, shardStr := range expectedShardStrs {
		found := false
		for _, tol := range tolerations {
			if tol.Key == "sharding.monosi.io/shard" && tol.Value == shardStr && tol.Operator == corev1.TolerationOpEqual {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing expected toleration for shard %s", shardStr)
		}
	}

	if err := client.Delete(ctx, &updatedCR); err != nil {
		t.Fatalf("failed to delete assignment: %v", err)
	}

	delRes, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile on deletion failed: %v", err)
	}
	if delRes.Requeue {
		t.Errorf("unexpected requeue on deletion: %v", delRes)
	}

	var cleanedDeploy appsv1.Deployment
	if err := client.Get(ctx, types.NamespacedName{Namespace: "default", Name: "tenant-alpha-worker"}, &cleanedDeploy); err != nil {
		t.Fatalf("failed to fetch cleaned deployment: %v", err)
	}

	if _, ok := cleanedDeploy.Spec.Template.Labels[LabelTenant]; ok {
		t.Errorf("expected LabelTenant to be removed on deletion")
	}
	if _, ok := cleanedDeploy.Spec.Template.Labels[LabelSharded]; ok {
		t.Errorf("expected LabelSharded to be removed on deletion")
	}
	if _, ok := cleanedDeploy.Spec.Template.Annotations[AnnotationShards]; ok {
		t.Errorf("expected AnnotationShards to be removed on deletion")
	}
	if cleanedDeploy.Spec.Template.Spec.Affinity != nil && cleanedDeploy.Spec.Template.Spec.Affinity.NodeAffinity != nil {
		if cleanedDeploy.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			t.Errorf("expected NodeAffinity to be removed on deletion")
		}
	}
	for _, tol := range cleanedDeploy.Spec.Template.Spec.Tolerations {
		if tol.Key == "sharding.monosi.io/shard" {
			t.Errorf("expected toleration for sharding.monosi.io/shard to be removed on deletion")
		}
	}

	var finalCR v1alpha1.ShuffleShardAssignment
	if err := client.Get(ctx, req.NamespacedName, &finalCR); err == nil {
		if controllerutil.ContainsFinalizer(&finalCR, FinalizerName) {
			t.Errorf("expected finalizer to be removed after cleanup")
		}
	}
}

func TestReconcileInvalidSpec(t *testing.T) {
	scheme := setupTestScheme(t)

	assignment := &v1alpha1.ShuffleShardAssignment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-invalid",
			Namespace: "default",
		},
		Spec: v1alpha1.ShuffleShardAssignmentSpec{
			TenantID:        "",
			ShardPoolSize:   8,
			ShardsPerTenant: 2,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(assignment).
		WithStatusSubresource(assignment).
		Build()

	reconciler := &ShuffleShardAssignmentReconciler{Client: client}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-invalid"}}

	_, err := reconciler.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("expected graceful handling of invalid spec, got error: %v", err)
	}

	var updated v1alpha1.ShuffleShardAssignment
	_ = client.Get(context.Background(), req.NamespacedName, &updated)
	if updated.Status.Phase != "Failed" {
		t.Errorf("expected phase Failed, got %s", updated.Status.Phase)
	}
}
