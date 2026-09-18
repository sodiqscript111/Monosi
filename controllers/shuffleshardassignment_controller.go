package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "monosi.io/shuffle-sharding-controller/api/v1alpha1"
	"monosi.io/shuffle-sharding-controller/pkg/sharder"
)

const (
	FinalizerName          = "sharding.monosi.io/finalizer"
	DefaultNodeSelectorKey = "topology.kubernetes.io/shard"
	DefaultShardPoolSize   = 8
	DefaultShardsPerTenant = 2

	LabelTenant         = "sharding.monosi.io/tenant"
	LabelSharded        = "sharding.monosi.io/sharded"
	AnnotationShards    = "sharding.monosi.io/assigned-shards"
	ConditionTypeReady  = "Ready"
	ConditionWorkloadOK = "WorkloadConfigured"
)

type ShuffleShardAssignmentReconciler struct {
	client.Client
}

func (r *ShuffleShardAssignmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reqLog := log.FromContext(ctx)

	var assignment v1alpha1.ShuffleShardAssignment
	if err := r.Get(ctx, req.NamespacedName, &assignment); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		reqLog.Error(err, "unable to fetch ShuffleShardAssignment")
		return ctrl.Result{}, err
	}

	if !assignment.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&assignment, FinalizerName) {
			if err := r.cleanupTargetWorkload(ctx, &assignment); err != nil {
				reqLog.Error(err, "failed to cleanup target workload on deletion")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}
			controllerutil.RemoveFinalizer(&assignment, FinalizerName)
			if err := r.Update(ctx, &assignment); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&assignment, FinalizerName) {
		controllerutil.AddFinalizer(&assignment, FinalizerName)
		if err := r.Update(ctx, &assignment); err != nil {
			return ctrl.Result{}, err
		}
	}

	poolSize := assignment.Spec.ShardPoolSize
	if poolSize <= 0 {
		poolSize = DefaultShardPoolSize
	}
	shardsPerTenant := assignment.Spec.ShardsPerTenant
	if shardsPerTenant <= 0 {
		shardsPerTenant = DefaultShardsPerTenant
	}
	nodeKey := assignment.Spec.NodeSelectorKey
	if nodeKey == "" {
		nodeKey = DefaultNodeSelectorKey
	}

	assignedShards, err := sharder.ComputeAssignment(assignment.Spec.TenantID, poolSize, shardsPerTenant)
	if err != nil {
		reqLog.Error(err, "failed to compute deterministic shard assignment", "tenantID", assignment.Spec.TenantID)
		changed := r.setCondition(&assignment, metav1.Condition{
			Type:               ConditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             "CalculationFailed",
			Message:            err.Error(),
			ObservedGeneration: assignment.Generation,
		})
		if assignment.Status.Phase != "Failed" {
			assignment.Status.Phase = "Failed"
			changed = true
		}
		if changed {
			now := metav1.Now()
			assignment.Status.LastReconciled = &now
			if updateErr := r.Status().Update(ctx, &assignment); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
		}
		return ctrl.Result{}, nil
	}

	statusChanged := false
	if !reflect.DeepEqual(assignment.Status.AssignedShards, assignedShards) {
		assignment.Status.AssignedShards = assignedShards
		statusChanged = true
	}
	if assignment.Status.Phase != "Assigned" {
		assignment.Status.Phase = "Assigned"
		statusChanged = true
	}
	if assignment.Status.ObservedGeneration != assignment.Generation {
		assignment.Status.ObservedGeneration = assignment.Generation
		statusChanged = true
	}

	if r.setCondition(&assignment, metav1.Condition{
		Type:               ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             "ShardsAssigned",
		Message:            fmt.Sprintf("Tenant deterministically assigned to shards: %v", assignedShards),
		ObservedGeneration: assignment.Generation,
	}) {
		statusChanged = true
	}

	if assignment.Spec.TargetWorkload != nil {
		workloadErr := r.reconcileTargetWorkload(ctx, &assignment, assignedShards, nodeKey)
		if workloadErr != nil {
			reqLog.Error(workloadErr, "failed to apply shard placement to target workload")
			if r.setCondition(&assignment, metav1.Condition{
				Type:               ConditionWorkloadOK,
				Status:             metav1.ConditionFalse,
				Reason:             "WorkloadUpdateFailed",
				Message:            workloadErr.Error(),
				ObservedGeneration: assignment.Generation,
			}) {
				statusChanged = true
			}
			if statusChanged {
				now := metav1.Now()
				assignment.Status.LastReconciled = &now
				_ = r.Status().Update(ctx, &assignment)
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, workloadErr
		}

		if r.setCondition(&assignment, metav1.Condition{
			Type:               ConditionWorkloadOK,
			Status:             metav1.ConditionTrue,
			Reason:             "WorkloadConfigured",
			Message:            fmt.Sprintf("Workload %s configured with shards %v", assignment.Spec.TargetWorkload.Name, assignedShards),
			ObservedGeneration: assignment.Generation,
		}) {
			statusChanged = true
		}
	}

	if statusChanged {
		now := metav1.Now()
		assignment.Status.LastReconciled = &now
		if err := r.Status().Update(ctx, &assignment); err != nil {
			reqLog.Error(err, "failed to update ShuffleShardAssignment status")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *ShuffleShardAssignmentReconciler) cleanupTargetWorkload(
	ctx context.Context,
	assignment *v1alpha1.ShuffleShardAssignment,
) error {
	target := assignment.Spec.TargetWorkload
	if target == nil {
		return nil
	}

	targetNamespace := target.Namespace
	if targetNamespace == "" {
		targetNamespace = assignment.Namespace
	}

	kind := target.Kind
	if kind == "" {
		kind = "Deployment"
	}
	if kind != "Deployment" {
		return nil
	}

	var deploy appsv1.Deployment
	deployKey := types.NamespacedName{Namespace: targetNamespace, Name: target.Name}
	if err := r.Get(ctx, deployKey, &deploy); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	nodeKey := assignment.Spec.NodeSelectorKey
	if nodeKey == "" {
		nodeKey = DefaultNodeSelectorKey
	}

	modified := false

	if deploy.Spec.Template.Labels != nil {
		if _, ok := deploy.Spec.Template.Labels[LabelTenant]; ok {
			delete(deploy.Spec.Template.Labels, LabelTenant)
			modified = true
		}
		if _, ok := deploy.Spec.Template.Labels[LabelSharded]; ok {
			delete(deploy.Spec.Template.Labels, LabelSharded)
			modified = true
		}
	}
	if deploy.Spec.Template.Annotations != nil {
		if _, ok := deploy.Spec.Template.Annotations[AnnotationShards]; ok {
			delete(deploy.Spec.Template.Annotations, AnnotationShards)
			modified = true
		}
	}

	podSpec := &deploy.Spec.Template.Spec
	if podSpec.Affinity != nil && podSpec.Affinity.NodeAffinity != nil &&
		podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		nodeSelector := podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		var newTerms []corev1.NodeSelectorTerm
		for _, term := range nodeSelector.NodeSelectorTerms {
			var newExprs []corev1.NodeSelectorRequirement
			for _, expr := range term.MatchExpressions {
				if expr.Key != nodeKey {
					newExprs = append(newExprs, expr)
				} else {
					modified = true
				}
			}
			if len(newExprs) > 0 || len(term.MatchFields) > 0 {
				term.MatchExpressions = newExprs
				newTerms = append(newTerms, term)
			} else {
				modified = true
			}
		}
		nodeSelector.NodeSelectorTerms = newTerms
		if len(newTerms) == 0 {
			podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nil
			if podSpec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution == nil {
				podSpec.Affinity.NodeAffinity = nil
			}
			if podSpec.Affinity.PodAffinity == nil && podSpec.Affinity.PodAntiAffinity == nil && podSpec.Affinity.NodeAffinity == nil {
				podSpec.Affinity = nil
			}
		}
	}

	if len(podSpec.Tolerations) > 0 {
		var newTols []corev1.Toleration
		for _, tol := range podSpec.Tolerations {
			if tol.Key == nodeKey {
				modified = true
			} else {
				newTols = append(newTols, tol)
			}
		}
		podSpec.Tolerations = newTols
	}

	if modified {
		return r.Update(ctx, &deploy)
	}

	return nil
}

func (r *ShuffleShardAssignmentReconciler) reconcileTargetWorkload(
	ctx context.Context,
	assignment *v1alpha1.ShuffleShardAssignment,
	assignedShards []int,
	nodeKey string,
) error {
	target := assignment.Spec.TargetWorkload
	if target == nil {
		return nil
	}

	targetNamespace := target.Namespace
	if targetNamespace == "" {
		targetNamespace = assignment.Namespace
	}

	kind := target.Kind
	if kind == "" {
		kind = "Deployment"
	}

	if kind != "Deployment" {
		return fmt.Errorf("unsupported workload kind: %s (only Deployment is currently supported)", kind)
	}

	var deploy appsv1.Deployment
	deployKey := types.NamespacedName{Namespace: targetNamespace, Name: target.Name}
	if err := r.Get(ctx, deployKey, &deploy); err != nil {
		return err
	}

	shardValues := make([]string, len(assignedShards))
	for i, s := range assignedShards {
		shardValues[i] = strconv.Itoa(s)
	}
	shardsAnnotationVal := strings.Join(shardValues, ",")

	modified := false

	if deploy.Spec.Template.Labels == nil {
		deploy.Spec.Template.Labels = make(map[string]string)
	}
	if deploy.Spec.Template.Labels[LabelTenant] != assignment.Spec.TenantID {
		deploy.Spec.Template.Labels[LabelTenant] = assignment.Spec.TenantID
		modified = true
	}
	if deploy.Spec.Template.Labels[LabelSharded] != "true" {
		deploy.Spec.Template.Labels[LabelSharded] = "true"
		modified = true
	}
	if deploy.Spec.Template.Annotations == nil {
		deploy.Spec.Template.Annotations = make(map[string]string)
	}
	if deploy.Spec.Template.Annotations[AnnotationShards] != shardsAnnotationVal {
		deploy.Spec.Template.Annotations[AnnotationShards] = shardsAnnotationVal
		modified = true
	}

	podSpec := &deploy.Spec.Template.Spec
	if podSpec.Affinity == nil {
		podSpec.Affinity = &corev1.Affinity{}
	}
	if podSpec.Affinity.NodeAffinity == nil {
		podSpec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	if podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{},
		}
	}

	nodeSelector := podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	expectedReq := corev1.NodeSelectorRequirement{
		Key:      nodeKey,
		Operator: corev1.NodeSelectorOpIn,
		Values:   shardValues,
	}

	if len(nodeSelector.NodeSelectorTerms) == 0 {
		nodeSelector.NodeSelectorTerms = []corev1.NodeSelectorTerm{
			{
				MatchExpressions: []corev1.NodeSelectorRequirement{expectedReq},
			},
		}
		modified = true
	} else {
		found := false
		for i := range nodeSelector.NodeSelectorTerms {
			term := &nodeSelector.NodeSelectorTerms[i]
			for j := range term.MatchExpressions {
				if term.MatchExpressions[j].Key == nodeKey {
					found = true
					if !reflect.DeepEqual(term.MatchExpressions[j], expectedReq) {
						term.MatchExpressions[j] = expectedReq
						modified = true
					}
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			nodeSelector.NodeSelectorTerms[0].MatchExpressions = append(
				nodeSelector.NodeSelectorTerms[0].MatchExpressions,
				expectedReq,
			)
			modified = true
		}
	}

	for _, shardStr := range shardValues {
		hasToleration := false
		for _, tol := range podSpec.Tolerations {
			if tol.Key == nodeKey && tol.Operator == corev1.TolerationOpEqual && tol.Value == shardStr && tol.Effect == corev1.TaintEffectNoSchedule {
				hasToleration = true
				break
			}
		}
		if !hasToleration {
			podSpec.Tolerations = append(podSpec.Tolerations, corev1.Toleration{
				Key:      nodeKey,
				Operator: corev1.TolerationOpEqual,
				Value:    shardStr,
				Effect:   corev1.TaintEffectNoSchedule,
			})
			modified = true
		}
	}

	if modified {
		return r.Update(ctx, &deploy)
	}

	return nil
}

func (r *ShuffleShardAssignmentReconciler) setCondition(assignment *v1alpha1.ShuffleShardAssignment, newCond metav1.Condition) bool {
	for i, cond := range assignment.Status.Conditions {
		if cond.Type == newCond.Type {
			if cond.Status == newCond.Status && cond.Reason == newCond.Reason && cond.Message == newCond.Message {
				return false
			}
			newCond.LastTransitionTime = metav1.Now()
			assignment.Status.Conditions[i] = newCond
			return true
		}
	}
	newCond.LastTransitionTime = metav1.Now()
	assignment.Status.Conditions = append(assignment.Status.Conditions, newCond)
	return true
}

func (r *ShuffleShardAssignmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.ShuffleShardAssignment{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}
