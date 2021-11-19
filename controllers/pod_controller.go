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
	"fmt"

	"github.com/go-logr/logr"
	"github.com/porter-dev/porter-agent/pkg/models"
	"github.com/porter-dev/porter-agent/pkg/processor"
	"github.com/porter-dev/porter-agent/pkg/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// PodReconciler reconciles a Pod object
type PodReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Processor processor.Interface

	logger logr.Logger
}

//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=core,resources=pods/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Pod object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.8.3/pkg/reconcile
func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.logger = log.FromContext(ctx)

	instance := &corev1.Pod{}
	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// must have been a delete event
			r.logger.Info("pod deleted")

			// TODO: triggerNotify
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	// else we still have the object
	// we can log the current condition
	if instance.Status.Phase == corev1.PodFailed ||
		instance.Status.Phase == corev1.PodUnknown {
		// critical condition, must trigger a notification
		r.addToQueue(ctx, req, instance, true)
		return ctrl.Result{}, nil
	}

	// check latest condition by sorting
	// r.logger.Info("pod conditions before sorting", "conditions", instance.Status.Conditions)
	utils.PodConditionsSorter(instance.Status.Conditions, true)
	// r.logger.Info("pod conditions after sorting", "conditions", instance.Status.Conditions)

	// in case status conditions are not yet set for the pod
	// reconcile the event
	if len(instance.Status.Conditions) == 0 {
		r.logger.Info("empty status conditions....reconciling")
		return ctrl.Result{Requeue: true}, nil
	}

	latestCondition := instance.Status.Conditions[0]

	if latestCondition.Status == corev1.ConditionFalse {
		// latest condition status is false, hence trigger notification
		r.addToQueue(ctx, req, instance, true)
	} else {
		// trigger with critical false
		r.addToQueue(ctx, req, instance, false)
	}

	// fetch and enqueue latest logs
	r.logger.Info("processing logs for pod", "status", instance.Status)
	if len(instance.Spec.Containers) > 1 {
		r.Processor.EnqueueDetails(ctx, req.NamespacedName, &processor.EnqueueDetailOptions{
			ContainerNamesToFetchLogs: []string{instance.Spec.Containers[0].Name},
		})
	} else {
		r.Processor.EnqueueDetails(ctx, req.NamespacedName, &processor.EnqueueDetailOptions{})
	}

	return ctrl.Result{}, nil
}

func (r *PodReconciler) addToQueue(ctx context.Context, req ctrl.Request, instance *corev1.Pod, isCritical bool) {
	reason, message := r.getReasonAndMessage(instance)
	eventDetails := &models.EventDetails{
		ResourceType: models.PodResource,
		Name:         req.Name,
		Namespace:    req.Namespace,
		Message:      message,
		Reason:       reason,
		Critical:     isCritical,
		Timestamp:    getTime(),
		Phase:        string(instance.Status.Phase),
		Status:       fmt.Sprintf("Type: %s, Status: %s", instance.Status.Conditions[0].Type, instance.Status.Conditions[0].Status),
	}

	r.populateOwnerDetails(ctx, req, instance, eventDetails)
	r.logger.Info("populated owner details", "details", eventDetails)

	r.Processor.AddToWorkQueue(ctx, req.NamespacedName, eventDetails)
}

func (r *PodReconciler) getReasonAndMessage(instance *corev1.Pod) (string, string) {
	// since list is already sorted in place now, hence the first condition
	// is the latest, get its reason and message

	latest := instance.Status.Conditions[0]
	return latest.Reason, latest.Message
}

func (r *PodReconciler) fetchReplicaSetOwner(ctx context.Context, req ctrl.Request) (*metav1.OwnerReference, error) {
	rs := &appsv1.ReplicaSet{}

	err := r.Client.Get(ctx, req.NamespacedName, rs)
	if err != nil {
		r.logger.Error(err, "cannot fetch replicaset object")
		return nil, err
	}

	// get replicaset owner
	owners := rs.ObjectMeta.OwnerReferences
	if len(owners) == 0 {
		r.logger.Info("no owner for teh replicaset", "replicaset name", req.NamespacedName)
		return nil, fmt.Errorf("no owner defined for replicaset")
	}

	owner := owners[0]
	return &owner, nil
}

func (r *PodReconciler) populateOwnerDetails(ctx context.Context, req ctrl.Request, pod *corev1.Pod, eventDetails *models.EventDetails) {
	owners := pod.ObjectMeta.OwnerReferences

	if len(owners) == 0 {
		r.logger.Info("no owners defined for the pod")
		return
	}

	// in case of multiple owners, take the first
	var owner *metav1.OwnerReference
	var err error

	owner = &owners[0]

	if owner.Kind == "ReplicaSet" {
		r.logger.Info("fetching owner for replicaset")
		owner, err = r.fetchReplicaSetOwner(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      owner.Name,
				Namespace: req.Namespace,
			},
		})

		if err != nil {
			r.logger.Error(err, "cannot fetch owner for replicaset")

			return
		}
	}

	eventDetails.OwnerName = owner.Name
	eventDetails.OwnerType = owner.Kind
}

// SetupWithManager sets up the controller with the Manager.
func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Complete(r)
}
