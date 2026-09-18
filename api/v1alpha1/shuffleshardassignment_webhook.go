package v1alpha1

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type ShuffleShardAssignmentCustomValidator struct{}

func SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&ShuffleShardAssignment{}).
		WithValidator(&ShuffleShardAssignmentCustomValidator{}).
		Complete()
}

func (v *ShuffleShardAssignmentCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	assignment, ok := obj.(*ShuffleShardAssignment)
	if !ok {
		return nil, fmt.Errorf("expected ShuffleShardAssignment but got %T", obj)
	}
	return nil, ValidateShuffleShardAssignment(assignment)
}

func (v *ShuffleShardAssignmentCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	assignment, ok := newObj.(*ShuffleShardAssignment)
	if !ok {
		return nil, fmt.Errorf("expected ShuffleShardAssignment but got %T", newObj)
	}
	return nil, ValidateShuffleShardAssignment(assignment)
}

func (v *ShuffleShardAssignmentCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func ValidateShuffleShardAssignment(a *ShuffleShardAssignment) error {
	var allErrs field.ErrorList
	specPath := field.NewPath("spec")

	if a.Spec.TenantID == "" {
		allErrs = append(allErrs, field.Required(specPath.Child("tenantID"), "tenantID must not be empty"))
	}

	poolSize := a.Spec.ShardPoolSize
	if poolSize < 1 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("shardPoolSize"), poolSize, "shardPoolSize must be at least 1"))
	}

	shardsPerTenant := a.Spec.ShardsPerTenant
	if shardsPerTenant < 1 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("shardsPerTenant"), shardsPerTenant, "shardsPerTenant must be at least 1"))
	} else if poolSize >= 1 && shardsPerTenant > poolSize {
		allErrs = append(allErrs, field.Invalid(specPath.Child("shardsPerTenant"), shardsPerTenant, "shardsPerTenant cannot exceed shardPoolSize"))
	}

	if len(allErrs) > 0 {
		return apierrors.NewInvalid(
			schema.GroupKind{Group: "sharding.monosi.io", Kind: "ShuffleShardAssignment"},
			a.Name,
			allErrs,
		)
	}
	return nil
}
