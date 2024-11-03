package controller

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/pulumi/pulumi/sdk/v3/go/auto"
	"github.com/pulumi/pulumi/sdk/v3/go/auto/optdestroy"
	"github.com/pulumi/pulumi/sdk/v3/go/auto/optup"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/vvbogdanov87/acme-common/pkg/stack"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// conditionTypeReady represents the status of the Object reconciliation
	conditionTypeReady = "Ready"

	finalizerSuffix = ".cloud.acme.local/finalizer"
)

// Obj interface extends the client.Object interface so we can handle the status of the Object
type Obj interface {
	client.Object

	// GetStatusConditions returns the pointer ro the Condtions slice
	// so that we can update the Ready condition of the Object
	GetStatusConditions() *[]metav1.Condition

	// SetStatus sets the fields of the status after the stack is created/updated
	SetStatus(auto.OutputMap)

	// GetPulumiProgram defines the program that creates pulumi resources.
	GetPulumiProgram() pulumi.RunFunc
}

func Reconcile(ctx context.Context, o Obj, req ctrl.Request, r client.Client) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the Object instance
	if err := r.Get(ctx, req.NamespacedName, o); err != nil {
		log.Error(err, "unable to fetch Object")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Set the Ready condition to Flase when no status is available
	if len(*o.GetStatusConditions()) == 0 {
		meta.SetStatusCondition(
			o.GetStatusConditions(),
			metav1.Condition{
				Type:    conditionTypeReady,
				Status:  metav1.ConditionFalse,
				Reason:  "Reconciling",
				Message: "Starting reconciliation",
			},
		)
		if err := r.Status().Update(ctx, o); err != nil {
			log.Error(err, "Failed to update Object status")
			return ctrl.Result{}, err
		}

		// Re-fetch the Object after updating the status
		if err := r.Get(ctx, req.NamespacedName, o); err != nil {
			log.Error(err, "Failed to re-fetch Object")
			return ctrl.Result{}, err
		}
	}

	// Add a finalizer
	kind := strings.ToLower(o.GetObjectKind().GroupVersionKind().Kind)
	finalizer := kind + finalizerSuffix

	if !controllerutil.ContainsFinalizer(o, finalizer) {
		log.Info("Adding Finalizer for Object")
		controllerutil.AddFinalizer(o, finalizer)

		if err := r.Update(ctx, o); err != nil {
			log.Error(err, "Failed to update custom resource to add finalizer")
			return ctrl.Result{}, err
		}
	}

	// Initialize the stack
	program := o.GetPulumiProgram()

	s, err := stack.GetStack(ctx, program, kind, o.GetName(), o.GetNamespace())
	if err != nil {
		log.Error(err, "failed to create stack")
		return ctrl.Result{}, nil
	}

	// Check if the Object instance is marked to be deleted, which is
	// indicated by the deletion timestamp being set.
	if o.GetDeletionTimestamp() != nil {
		if controllerutil.ContainsFinalizer(o, finalizer) {
			log.Info("Performing Finalizer Operations for Object before delete CR")

			// Set the Ready condition to "False" to reflect that this resource began its process to be terminated.
			meta.SetStatusCondition(o.GetStatusConditions(), metav1.Condition{
				Type:    conditionTypeReady,
				Status:  metav1.ConditionFalse,
				Reason:  "Finalizing",
				Message: fmt.Sprintf("Performing finalizer operations for the custom resource: %s ", o.GetName()),
			})

			if err := r.Status().Update(ctx, o); err != nil {
				log.Error(err, "Failed to update Object status")
				return ctrl.Result{}, err
			}

			// Delete the stack
			outBuf := new(bytes.Buffer)
			_, err := s.Destroy(ctx, optdestroy.ProgressStreams(outBuf))
			if err != nil {
				log.Error(err, "Failed to destroy stack")
				return ctrl.Result{}, nil
			}
			log.Info("successfully destroyed stack", s.Name(), outBuf.String())

			// Re-fetch the Object before updating the status
			if err := r.Get(ctx, req.NamespacedName, o); err != nil {
				log.Error(err, "Failed to re-fetch Object")
				return ctrl.Result{}, err
			}

			meta.SetStatusCondition(o.GetStatusConditions(), metav1.Condition{
				Type:   conditionTypeReady,
				Status: metav1.ConditionFalse,
				Reason: "Finalizing",
				Message: fmt.Sprintf(
					"Finalizer operations for custom resource %s name were successfully accomplished",
					o.GetName(),
				),
			})

			if err := r.Status().Update(ctx, o); err != nil {
				log.Error(err, "Failed to update Object status")
				return ctrl.Result{}, err
			}

			log.Info("Removing Finalizer for Object after successfully perform the operations")
			if ok := controllerutil.RemoveFinalizer(o, finalizer); !ok {
				log.Error(err, "Failed to remove finalizer for Object")
				return ctrl.Result{Requeue: true}, nil
			}

			if err := r.Update(ctx, o); err != nil {
				log.Error(err, "Failed to remove finalizer for Object")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Create or update the stack
	// we'll write all of the update logs to a buffer
	outBuf := new(bytes.Buffer)
	upRes, err := s.Up(ctx, optup.ProgressStreams(outBuf))
	if err != nil {
		log.Error(err, "failed to deploy stack", s.Name(), outBuf.String())
		return ctrl.Result{}, err
	}
	log.Info("successfully deployed/updated stack", s.Name(), outBuf.String())

	o.SetStatus(upRes.Outputs)

	if err = r.Status().Update(ctx, o); err != nil {
		log.Error(err, "Failed to update Object status")
		return ctrl.Result{}, err
	}

	// Set the Ready condtion to "True" to reflect that this resource is created.
	meta.SetStatusCondition(o.GetStatusConditions(), metav1.Condition{
		Type:    conditionTypeReady,
		Status:  metav1.ConditionTrue,
		Reason:  "Created",
		Message: "The Object was successfully created",
	})

	if err := r.Status().Update(ctx, o); err != nil {
		log.Error(err, "Failed to update Object status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}
