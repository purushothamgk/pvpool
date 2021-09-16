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

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pvpoolv1 "github.com/noobaa/pv-pool-operator/api/v1"
)

const (
	dataMountPath    = "/data"
	storageAgentPort = 8080
)

// storageAgentStatus is the status returned by storage agent
type storageAgentStatus struct {
	Name  string `json:"name"`
	Total int64  `json:"total"`
	Used  int64  `json:"used"`
	State string `json:"state"`
}

// PvPoolReconciler reconciles a PvPool object
type PvPoolReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

func doNotRequeue() (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// reques the request with no error after the given number of seconds delay
func requeueAfterSeconds(seconds time.Duration) (ctrl.Result, error) {
	return ctrl.Result{RequeueAfter: seconds * time.Second}, nil
}

// requeue with error after a 3 seconds delay
func requeueWithError(err error) (ctrl.Result, error) {
	return ctrl.Result{RequeueAfter: 3 * time.Second}, err
}

// +kubebuilder:rbac:groups=pvpool.noobaa.com,resources=pvpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pvpool.noobaa.com,resources=pvpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pvpool.noobaa.com,resources=pvpools/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=services,verbs=*
// +kubebuilder:rbac:groups="",resources=pods,verbs=*
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=*

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the PvPool object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.7.0/pkg/reconcile
func (r *PvPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	r.Log.Info("Starting reconcile..", "Request", req)

	pvPool := &pvpoolv1.PvPool{}

	// Fetch the PV pool resource according to the namespaced name in the request
	err := r.Get(ctx, req.NamespacedName, pvPool)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			r.Log.Info("PVPool resource not found. Ignoring since object must be deleted")
			return doNotRequeue()
		}
		// Error reading the object - requeue the request.
		r.Log.Error(err, "Failed to get PVPool")
		return requeueWithError(err)
	}

	newStatus, reconcileErr := r.reconcilePvPool(pvPool)

	// Update status if needed
	if !reflect.DeepEqual(newStatus, pvPool.Status) {
		pvPool.Status = *newStatus
		err := r.Status().Update(ctx, pvPool)
		if err != nil {
			r.Log.Error(err, "Failed to update pvPool status", "newStatus", newStatus)
			return requeueWithError(err)
		}
	}

	if reconcileErr != nil {
		return requeueWithError(err)
	}

	if pvPool.Status.Phase != pvpoolv1.PvPoolPhaseReady {
		r.Log.Info("pvpool is not in Ready phase yet. requeing in 3 seconds", "Phase", pvPool.Status.Phase)
		// if phase is not ready yet, requeue after 3 seconds
		return requeueAfterSeconds(3)
	}
	// when phase is ready reques after 1 minute
	r.Log.Info("pvpool reached Ready phase yet. requeing in 60 seconds", "Phase", pvPool.Status.Phase)
	return requeueAfterSeconds(60)
}

func (r *PvPoolReconciler) reconcilePvPool(pvp *pvpoolv1.PvPool) (*pvpoolv1.PvPoolStatus, error) {

	// init a status struct to update with the status
	newStatus := &pvpoolv1.PvPoolStatus{
		Phase:        pvpoolv1.PvPoolPhaseUnknown,
		PodsInfo:     make([]pvpoolv1.PvPodSInfo, 0),
		CountByState: make(map[pvpoolv1.PvPodStatus]int32),
	}

	srv, err := r.ensurePvPoolService(pvp)
	if err != nil {
		return newStatus, err
	}

	sts, err := r.ensurePvPoolStatefulset(pvp)
	if err != nil {
		return newStatus, err
	}

	// get the pods status and continue accordingly
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(pvp.Namespace),
		client.MatchingLabels(r.getPvPoolLabels(pvp)),
	}
	err = r.List(context.TODO(), podList, listOpts...)
	if err != nil {
		r.Log.Error(err, "Failed to list pods")
		return newStatus, err
	}

	err = r.collectPodsStatus(newStatus, podList)
	if err != nil {
		r.Log.Error(err, "Failed to get storage agents status")
		return newStatus, err
	}

	err = r.reconcilePvPoolStatefulset(pvp, sts, srv, newStatus, podList)
	if err != nil {
		// Failed to reconcile the PvPool Statefulset
		return newStatus, err
	}

	return newStatus, nil

}

// returns the conventional service name for the reconciled PV pool
func (r *PvPoolReconciler) getPvPoolServiceName(pvp *pvpoolv1.PvPool) string {
	return pvp.Name + "-srv"
}

func (r *PvPoolReconciler) ensurePvPoolService(pvp *pvpoolv1.PvPool) (*corev1.Service, error) {

	pvPoolSrv := &corev1.Service{}
	srvNamespacedName := types.NamespacedName{Namespace: pvp.Namespace, Name: r.getPvPoolServiceName(pvp)}

	err := r.Get(context.TODO(), srvNamespacedName, pvPoolSrv)
	if err != nil {
		if errors.IsNotFound(err) {
			// service is not found. create a new service
			r.Log.Info("PVPool service not found. will create a new one")
			pvPoolSrv = r.newServiceForPvPool(pvp)
			err := r.Create(context.TODO(), pvPoolSrv)
			if err != nil {
				r.Log.Error(err, "got error on service creation", "service name", r.getPvPoolServiceName(pvp))
				return pvPoolSrv, err
			}
			r.Log.Info("created new service", "service name", r.getPvPoolServiceName(pvp))

			return pvPoolSrv, nil
		}
		// Error reading the object - requeue the request.
		r.Log.Error(err, "Failed to get PVPool")
		return nil, err
	}

	r.Log.Info("service already exist ", "service name", r.getPvPoolServiceName(pvp))
	return pvPoolSrv, nil
}

func (r *PvPoolReconciler) getPvPoolLabels(pvp *pvpoolv1.PvPool) map[string]string {
	return map[string]string{
		"pv-pool": pvp.Name,
	}
}

func (r *PvPoolReconciler) newServiceForPvPool(pvp *pvpoolv1.PvPool) *corev1.Service {
	pvPoolSrv := &corev1.Service{}
	pvPoolSrv.Name = r.getPvPoolServiceName(pvp)
	pvPoolSrv.Namespace = pvp.Namespace
	pvPoolSrv.Spec = corev1.ServiceSpec{
		Type:     corev1.ServiceTypeClusterIP,
		Selector: r.getPvPoolLabels(pvp),
		Ports: []corev1.ServicePort{{
			Port:       storageAgentPort,
			TargetPort: intstr.FromInt(storageAgentPort),
			Name:       "storage-agent-api",
		}},
	}

	// set this pvpool resources as the service owner
	ctrl.SetControllerReference(pvp, pvPoolSrv, r.Scheme)

	return pvPoolSrv
}

// returns the conventional statefulset name for the reconciled PV pool
func (r *PvPoolReconciler) getPvPoolStatefulsetName(pvp *pvpoolv1.PvPool) string {
	return pvp.Name + "-sts"
}

func (r *PvPoolReconciler) ensurePvPoolStatefulset(pvp *pvpoolv1.PvPool) (*appsv1.StatefulSet, error) {

	sts := &appsv1.StatefulSet{}
	stsNamespacedName := types.NamespacedName{Namespace: pvp.Namespace, Name: r.getPvPoolStatefulsetName(pvp)}

	// Fetch the statefulset that manages the PV pool pods
	err := r.Get(context.TODO(), stsNamespacedName, sts)
	if err != nil {
		if errors.IsNotFound(err) {
			// Statefulset is not found. create a new statefulset
			r.Log.Info("PVPool statefulset not found. will create a new on")
			sts = r.newStatefulsetForPvPool(pvp)

			err := r.Create(context.TODO(), sts)
			if err != nil {
				r.Log.Error(err, "got error on statefulset creation", "statefulset name", r.getPvPoolStatefulsetName(pvp))
				return nil, err
			}

			r.Log.Info("created new statefulset", "statefulset name", sts.Name)
			return sts, nil
		}

		// Error reading the object - requeue the request.
		r.Log.Error(err, "Failed to get PVPool Statefulset")
		return nil, err
	}

	r.Log.Info("found existing statefulset", "statefulset name", sts.Name)

	return sts, nil

}

func (r *PvPoolReconciler) newStatefulsetForPvPool(pvp *pvpoolv1.PvPool) *appsv1.StatefulSet {
	pvPoolSTS := &appsv1.StatefulSet{}
	pvPoolSTS.Name = r.getPvPoolStatefulsetName(pvp)
	pvPoolSTS.Namespace = pvp.Namespace
	replicas := pvp.Spec.NumPVs

	// resources limits requests. no need for higher values, to allow all pods to start on weak clusters
	resourcesReq := corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewScaledQuantity(int64(100), resource.Milli),
		corev1.ResourceMemory: *resource.NewScaledQuantity(int64(100), resource.Mega),
	}

	// convert the requested PV size to bytes
	pvSizeBytes := int64(pvp.Spec.PvSizeGB) * 1024 * 1024 * 1024

	pvPoolSTS.Spec = appsv1.StatefulSetSpec{
		Replicas: &replicas,
		Selector: &v1.LabelSelector{
			MatchLabels: r.getPvPoolLabels(pvp),
		},
		ServiceName: r.getPvPoolServiceName(pvp),
		Template: corev1.PodTemplateSpec{
			ObjectMeta: v1.ObjectMeta{
				Labels: r.getPvPoolLabels(pvp),
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "storage-agent",
						Image: pvp.Spec.Image,
						Env: []corev1.EnvVar{
							{
								Name:  "PV_PATH",
								Value: dataMountPath,
							},
						},
						Command: []string{"node", "storage-agent.js"},
						Resources: corev1.ResourceRequirements{
							Limits:   resourcesReq,
							Requests: resourcesReq,
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "vol",
								MountPath: dataMountPath,
							},
						},
					},
				},
			},
		},
		VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
			{
				ObjectMeta: v1.ObjectMeta{Name: "vol"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: *resource.NewQuantity(pvSizeBytes, resource.BinarySI),
						},
					},
				},
			},
		},
	}

	// set this pvpool resources as the statefulset owner
	ctrl.SetControllerReference(pvp, pvPoolSTS, r.Scheme)

	return pvPoolSTS
}

func (r *PvPoolReconciler) reconcilePvPoolStatefulset(pvp *pvpoolv1.PvPool, sts *appsv1.StatefulSet, srv *corev1.Service, newStatus *pvpoolv1.PvPoolStatus, list *corev1.PodList) error {

	r.Log.Info("reconcilePvPoolStatefulset ", "statefulset name", sts.Name)
	
	// the statefulset exists. reconcile the properties in the PV pool CR
	shouldUpdate := false
	if pvp.Spec.NumPVs >= *sts.Spec.Replicas && newStatus.CountByState[pvpoolv1.PvPodStatusReady] != pvp.Spec.NumPVs {
		r.Log.Info("Start scaling ", "statefulset name", sts.Name)
		shouldUpdate = true
		// set the number of replicas to the number of PVs
		sts.Spec.Replicas = &pvp.Spec.NumPVs
		// set the phase to "Scaling"
		newStatus.Phase = pvpoolv1.PvPoolPhaseScaling

	} else if pvp.Spec.NumPVs < *sts.Spec.Replicas {
		// Start scaling down
		r.Log.Info("Start scaling down ", "statefulset name", sts.Name)
		r.decommissionRequiredPods(pvp, sts, list)

	} else if newStatus.CountByState[pvpoolv1.PvPodStatusReady] == pvp.Spec.NumPVs {
		// in this case the sts is reconciled (numPvs == sts.Spec.Replicas) and all pods are ready
		// mark the status as ready
		newStatus.Phase = pvpoolv1.PvPoolPhaseReady
	} else {
		// in this case the sts is reconciled but not all pods are ready. mark as scaling
		newStatus.Phase = pvpoolv1.PvPoolPhaseScaling
	}

	if shouldUpdate {
		r.Log.Info("found differences between existing sts and the desired one. will update", "statefulset name", sts.Name)
		// update the STS
		err := r.Update(context.TODO(), sts)
		if err != nil {
			r.Log.Error(err, "failed to update pv pool statefulset")
			return err
		}
	}

	return nil

}

func (r *PvPoolReconciler) percent(part int64, all int64) int {
	p := ((float64(part) * float64(100)) / float64(all))
	return int(p)
}

func (r *PvPoolReconciler) collectPodsStatus(pvpStatus *pvpoolv1.PvPoolStatus, list *corev1.PodList) error {

	for _, pod := range list.Items {

		r.Log.Info("collectPodsStatus", "status", pod.Name)
		state := pvpoolv1.PvPodStatus(pvpoolv1.PvPodStatusUnknown)
		agentStatus, err := r.getStorageAgentStatus(r.getPodURL(pod.Name, pod.Spec.Subdomain, pod.Namespace))
		if err != nil {
			r.Log.Info("got error when trying to get storage agent status. setting the state to unknown", "pod name", pod.Name, "error", err)
			return err
		} else {
			state = pvpoolv1.PvPodStatus(agentStatus.State)
			r.Log.Info("pv got agentStatus", "status", agentStatus)
		}
		pvpStatus.PodsInfo = append(pvpStatus.PodsInfo, pvpoolv1.PvPodSInfo{PodName: pod.Name, PodStatus: state})
		pvpStatus.CountByState[state]++
		//Used storage in percentage.
		pvpStatus.Used = r.percent(agentStatus.Used, agentStatus.Total)
		r.Log.Info("Used storage in percentage", "Used", pvpStatus.Used)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PvPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pvpoolv1.PvPool{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}

func (r *PvPoolReconciler) getPodURL(podName string, serviceName string, namespace string) string {
	return fmt.Sprintf("http://%s.%s.%s.svc:%d", podName, serviceName, namespace, storageAgentPort)
}

// getStorageAgentStatus makes an http request to the storage agent to query the status
func (r *PvPoolReconciler) getStorageAgentStatus(url string) (*storageAgentStatus, error) {

	urlRoute := url + "/status"

	agentClient := http.Client{
		Timeout: time.Second * 2,
	}

	req, err := http.NewRequest(http.MethodGet, urlRoute, nil)
	if err != nil {
		return nil, err
	}

	res, err := agentClient.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		err := fmt.Errorf("storage agent did not retrun the expected status code. got statusCode=%v", res.StatusCode)
		return nil, err
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	status := &storageAgentStatus{}
	err = json.Unmarshal(body, status)
	if err != nil {
		return nil, err
	}

	return status, nil

}

func (r *PvPoolReconciler) decommissionStorageAgent(url string) error {

	urlRoute := url + "/manage-agent/decommission"

	agentClient := http.Client{
		Timeout: time.Second * 2,
	}

	req, err := http.NewRequest(http.MethodPut, urlRoute, nil)
	if err != nil {
		return err
	}

	res, err := agentClient.Do(req)
	if err != nil {
		return err
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

	return nil
}


func (r *PvPoolReconciler) getInstanceNumberString(p string, s string) (int, error) {
	numStr := strings.TrimPrefix(p, s + "-")
	return strconv.Atoi(numStr)
}

func (r *PvPoolReconciler) decommissionRequiredPods(pvp *pvpoolv1.PvPool, sts *appsv1.StatefulSet,list *corev1.PodList ) error {

	for _, pod := range list.Items {
		num, err := r.getInstanceNumberString(pod.Name, sts.Name)
		if err != nil {
			r.Log.Info("Pod has a wrong instance number", pod.Name, err)
			continue
		}

		state := pvpoolv1.PvPodStatus(pvpoolv1.PvPodStatusUnknown)
		agentStatus, err := r.getStorageAgentStatus(r.getPodURL(pod.Name, pod.Spec.Subdomain, pod.Namespace))
		if err != nil {
			r.Log.Info("got error when trying to get storage agent status. setting the state to unknown", "pod name", pod.Name, "error", err)
			return err
		} else {
			state = pvpoolv1.PvPodStatus(agentStatus.State)
			r.Log.Info("pv got agentStatus", "status", agentStatus)
		}
	
		if int32(num) >= pvp.Spec.NumPVs && state != pvpoolv1.PvPodStatusDecommissioning {
			url := r.getPodURL(pod.Name, pod.Spec.Subdomain, pod.Namespace)
			r.Log.Info("decommissionRequiredPods", "status", pod.Name)
			err := r.decommissionStorageAgent(url)
			if err != nil {
				r.Log.Info("got error when trying to decommision. setting the state to unknown", "pod name", pod.Name, "error", err)
				return err
			}


			podsInfo := pvp.Status.PodsInfo
			for _, pInfo := range podsInfo {
				req_num, _ := r.getInstanceNumberString(pInfo.PodName, sts.Name)
				if num == req_num {
					decomm_state := pvpoolv1.PvPodStatus(pvpoolv1.PvPodStatusDecommissioned)
					pInfo.PodStatus = decomm_state
				}
			}
		}
	}
	return nil
}
