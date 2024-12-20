package controller

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/pulumi/pulumi/sdk/v3/go/auto"
	"github.com/pulumi/pulumi/sdk/v3/go/auto/optdestroy"
	"github.com/pulumi/pulumi/sdk/v3/go/auto/optup"
	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/vvbogdanov87/acme-common/pkg/stack"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
		if apierrors.IsNotFound(err) {
			// If the custom resource is not found then it usually means that it was deleted or not created
			// In this way, we will stop the reconciliation
			log.Info("resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "unable to fetch Object")
		return ctrl.Result{}, err
	}

	// Set the Ready condition to Flase when no status is available
	if len(*o.GetStatusConditions()) == 0 {
		if err := setReadyStatus(ctx, metav1.ConditionFalse, o, r, "Reconciling", "Starting reconciliation"); err != nil {
			log.Error(err, "Failed to update Object status")
			return ctrl.Result{}, err
		}
	}

	kind := strings.ToLower(o.GetObjectKind().GroupVersionKind().Kind)

	// Initialize the stack
	program := o.GetPulumiProgram()
	s, err := stack.GetStack(ctx, program, kind, o.GetName(), o.GetNamespace())
	if err != nil {
		log.Error(err, "failed to create stack")
		return ctrl.Result{}, nil
	}

	finalizer := kind + finalizerSuffix

	// Check if the Object instance is marked to be deleted, which is
	// indicated by the deletion timestamp being set.
	if o.GetDeletionTimestamp() == nil {
		// Add a finalizer
		if !controllerutil.ContainsFinalizer(o, finalizer) {
			log.Info("Adding Finalizer for Object")
			controllerutil.AddFinalizer(o, finalizer)

			if err := r.Update(ctx, o); err != nil {
				log.Error(err, "Failed to update custom resource to add finalizer")
				return ctrl.Result{}, err
			}
		}
	} else {
		// Delete the stack
		if controllerutil.ContainsFinalizer(o, finalizer) {
			log.Info("Performing Finalizer Operations for Object before delete CR")

			// Set the Ready condition to "False" to reflect that this resource began its process to be terminated.
			if err := setReadyStatus(ctx, metav1.ConditionFalse, o, r, "Finalizing", fmt.Sprintf("Performing finalizer operations for the custom resource: %s", o.GetName())); err != nil {
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

			if err := setReadyStatus(ctx, metav1.ConditionFalse, o, r, "Finalizing", fmt.Sprintf("Finalizer operations for custom resource %s name were successfully accomplished", o.GetName())); err != nil {
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

	// Check if there are any changes to be applied
	p, err := s.Preview(ctx)
	if err != nil {
		log.Error(err, "failed to get pending changes", s.Name())
		return ctrl.Result{}, err
	}

	isChanged := false
	for op := range p.ChangeSummary {
		if op != apitype.OpSame {
			isChanged = true
			break
		}
	}
	if !isChanged {
		log.Info("no changes to apply", "stack", s.Name())
		return ctrl.Result{}, nil
	}
	log.Info("detected changes to apply", "stack", s.Name())
	// Set the Ready condition to "False" to reflect that this resource is being reconciled.
	if err := setReadyStatus(ctx, metav1.ConditionFalse, o, r, "Reconciling", "Starting reconciliation"); err != nil {
		log.Error(err, "Failed to update Object status")
		return ctrl.Result{}, err
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
	if err := setReadyStatus(ctx, metav1.ConditionTrue, o, r, "Created", "The Object was successfully created"); err != nil {
		log.Error(err, "Failed to update Object status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func setReadyStatus(ctx context.Context, status metav1.ConditionStatus, o Obj, r client.Client, resason, message string) error {
	if changed := meta.SetStatusCondition(o.GetStatusConditions(), metav1.Condition{
		Type:    conditionTypeReady,
		Status:  status,
		Reason:  resason,
		Message: message,
	}); !changed {
		return nil
	}

	if err := r.Status().Update(ctx, o); err != nil {
		return err
	}

	return nil
}
