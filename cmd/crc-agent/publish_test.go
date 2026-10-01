package main

import (
	"context"
	"testing"

	"github.com/caxu-rh/guestcluster-operator/internal/resources"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPublishHandoffOwnsSecretAndRejectsForeignOwner(t *testing.T) {
	ctx := context.Background()
	cfg := config{InstanceName: "guest", InstanceUID: "instance", Namespace: "tenant", ExpectedVMIUID: "vmi"}
	kc := fake.NewClientset()
	info := &clusterInfo{Kubeconfig: []byte("config"), OCPVersion: "4.16"}
	if err := publishRawKubeconfig(ctx, kc, cfg, info); err != nil {
		t.Fatal(err)
	}
	name := resources.RawKubeconfigSecretNameForVMI(cfg.InstanceName, cfg.ExpectedVMIUID)
	secret, err := kc.CoreV1().Secrets(cfg.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owner := metav1.GetControllerOf(secret)
	if owner == nil || string(owner.UID) != cfg.InstanceUID || owner.BlockOwnerDeletion != nil {
		t.Fatalf("incorrect handoff owner: %+v", owner)
	}
	if err := publishRawKubeconfig(ctx, kc, cfg, info); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	for _, refs := range [][]metav1.OwnerReference{nil, {{
		APIVersion: "guestcluster.opdev.io/v1alpha1", Kind: "ClusterInstance", Name: cfg.InstanceName, UID: "old",
	}}} {
		foreign := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cfg.Namespace, OwnerReferences: refs},
			Data:       map[string][]byte{"sentinel": []byte("unchanged")},
		}
		client := fake.NewClientset(foreign)
		if err := publishRawKubeconfig(ctx, client, cfg, info); err == nil {
			t.Fatal("foreign handoff overwritten")
		}
		current, err := client.CoreV1().Secrets(cfg.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil || string(current.Data["sentinel"]) != "unchanged" {
			t.Fatal("foreign secret changed")
		}
	}
}
