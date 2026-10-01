//go:build openshift

package openshift

import (
	"context"
	"strings"
	"testing"
)

func TestRenderDirectManager(t *testing.T) {
	repoRoot, err := findRepositoryRoot()
	if err != nil {
		t.Fatal(err)
	}
	kubectl := commandPath(t, "KUBECTL", "kubectl", repoRoot)
	kustomize := commandPath(t, "KUSTOMIZE", "kustomize", repoRoot)
	const managerImage = "registry.example.test/guestcluster-operator:unit-test"
	const agentImage = "registry.example.test/guestcluster-crc-agent:unit-test"

	manifest, err := renderDirectManager(
		context.Background(), repoRoot, kubectl, kustomize, managerImage, agentImage,
		directBundleNamespace, "openshift-e2e-image-builder-dockercfg",
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"name: gc-openshift-e2e-controller-manager",
		"namespace: gc-openshift-e2e-manager",
		"image: " + managerImage,
		"value: " + agentImage,
		"value: " + directBundleNamespace,
		"name: gc-openshift-e2e-crc-agent-instance-role",
		"name: openshift-e2e-image-builder-dockercfg",
	} {
		if !strings.Contains(string(manifest), want) {
			t.Errorf("rendered direct manager manifest does not contain %q", want)
		}
	}
}
