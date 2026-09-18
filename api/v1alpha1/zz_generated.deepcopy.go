package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (in *WorkloadReference) DeepCopyInto(out *WorkloadReference) {
	*out = *in
}

func (in *WorkloadReference) DeepCopy() *WorkloadReference {
	if in == nil {
		return nil
	}
	out := new(WorkloadReference)
	in.DeepCopyInto(out)
	return out
}

func (in *ShuffleShardAssignmentSpec) DeepCopyInto(out *ShuffleShardAssignmentSpec) {
	*out = *in
	if in.TargetWorkload != nil {
		in, out := &in.TargetWorkload, &out.TargetWorkload
		*out = new(WorkloadReference)
		(*in).DeepCopyInto(*out)
	}
}

func (in *ShuffleShardAssignmentSpec) DeepCopy() *ShuffleShardAssignmentSpec {
	if in == nil {
		return nil
	}
	out := new(ShuffleShardAssignmentSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *ShuffleShardAssignmentStatus) DeepCopyInto(out *ShuffleShardAssignmentStatus) {
	*out = *in
	if in.AssignedShards != nil {
		in, out := &in.AssignedShards, &out.AssignedShards
		*out = make([]int, len(*in))
		copy(*out, *in)
	}
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.LastReconciled != nil {
		in, out := &in.LastReconciled, &out.LastReconciled
		*out = (*in).DeepCopy()
	}
}

func (in *ShuffleShardAssignmentStatus) DeepCopy() *ShuffleShardAssignmentStatus {
	if in == nil {
		return nil
	}
	out := new(ShuffleShardAssignmentStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *ShuffleShardAssignment) DeepCopyInto(out *ShuffleShardAssignment) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *ShuffleShardAssignment) DeepCopy() *ShuffleShardAssignment {
	if in == nil {
		return nil
	}
	out := new(ShuffleShardAssignment)
	in.DeepCopyInto(out)
	return out
}

func (in *ShuffleShardAssignment) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *ShuffleShardAssignmentList) DeepCopyInto(out *ShuffleShardAssignmentList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]ShuffleShardAssignment, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *ShuffleShardAssignmentList) DeepCopy() *ShuffleShardAssignmentList {
	if in == nil {
		return nil
	}
	out := new(ShuffleShardAssignmentList)
	in.DeepCopyInto(out)
	return out
}

func (in *ShuffleShardAssignmentList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}
