/*
Copyright 2021.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// PvPoolSpec defines the desired state of PvPool
type PvPoolSpec struct {
	// Image is the container image of the pv pod
	Image string `json:"image"`

	// NumPVs is the number of PV pods that should be created
	NumPVs int32 `json:"numPVs"`

	// pvSizeGB represents the volume size in GB.
	PvSizeGB uint32 `json:"pvSizeGB"`

	// StorageClass is the name of the storage class to use for the PV's
	StorageClass string `json:"storageClass,omitempty"`
}

// PvPoolStatus defines the observed state of PvPool
type PvPoolStatus struct {
	// Phase is the PvPool phase
	Phase PvPoolPhase `json:"phase"`

	// PodsInfo holds a list of all pods and their status
	PodsInfo []PvPodSInfo `json:"podsInfo"`

	// CountByState holds the number of pods for each pod status
	CountByState map[PvPodStatus]int32 `json:"countByState"`

	// Used storage percentage of the pool
	Used int64 `json:"used"`
}

// PvPoolPhase is a string enum type for the reconcile phase of a pv pool
type PvPoolPhase string

const (
	// PvPoolPhaseScaling means that the pool is adding or removing pods and not yet reached the desired state
	PvPoolPhaseScaling = "Scaling"

	// PvPoolPhaseReady means that the pool is in its desired state
	PvPoolPhaseReady = "Ready"

	// PvPoolPhaseUnknown means that the state of the pvpool is unkown by the operator
	PvPoolPhaseUnknown = "Unknown"
)

// PvPodSInfo indicates the status of a PV pod
type PvPodSInfo struct {
	PodName   string      `json:"podName"`
	PodStatus PvPodStatus `json:"podStatus"`
}

// PvPodStatus is a string enum type for PvPodStatus reconcile status
type PvPodStatus string

// These are the valid phases:
const (
	// PvPodStatusInitializing means that the pv pod is running initialization code and is not yet ready operate
	PvPodStatusInitializing = "Initializing"

	// PvPodStatusReady means that the pod is ready and operating
	PvPodStatusReady = "Ready"

	// PvPodStatusReady means that the pod is in an unknown state
	PvPodStatusUnknown = "Unknown"

	// PvPodStatusDecommissioning means that the pod is being deleted
	PvPodStatusDecommissioning = "Decommissioning"

	// PvPodStatusDecommissioned means that the pod is being deleted
	PvPodStatusDecommissioned = "Decommissioned"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Number of PVs",type="number",JSONPath=".spec.numPVs",description="Number of PVs in the pool"
// +kubebuilder:printcolumn:name="PV Size GB",type="number",JSONPath=".spec.pvSizeGB",description="Min PV size in GB"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Phase"

// PvPool is the Schema for the pvpools API
type PvPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PvPoolSpec   `json:"spec,omitempty"`
	Status PvPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PvPoolList contains a list of PvPool
type PvPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PvPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PvPool{}, &PvPoolList{})
}
