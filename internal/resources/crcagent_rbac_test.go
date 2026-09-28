package resources

import (
	"strings"
	"testing"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

func TestCRCAgentNamesAndJobAccount(t *testing.T) {
	for _, name := range []string{
		"one", strings.Repeat("a", 55) + "x", strings.Repeat("a", 55) + "y",
		strings.Repeat("a", 31) + ".b", // binding prefix ends at the dot
		strings.Repeat("a", 39) + ".b", // account prefix ends at the dot
		strings.Repeat("a", 31) + "-b",
	} {
		account, binding := CRCAgentAccountName(name), CRCAgentBindingName(name)
		if len(account) > 63 || len(binding) > 63 {
			t.Fatalf("names too long: %s %s", account, binding)
		}
		for _, generated := range []string{account, binding} {
			if errs := validation.IsDNS1123Subdomain(generated); len(errs) != 0 {
				t.Errorf("invalid name %q from %q: %v", generated, name, errs)
			}
		}
		if account != CRCAgentAccountName(name) || binding != CRCAgentBindingName(name) {
			t.Fatal("unstable name")
		}
		instance := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant"}}
		job := BuildCRCAgentJob(instance, "ip", "vmi", "ssh", "key", "identity", "image", "api", "pull")
		if job.Spec.Template.Spec.ServiceAccountName != account {
			t.Fatalf("Job account: %s", job.Spec.Template.Spec.ServiceAccountName)
		}
	}
	if CRCAgentAccountName(strings.Repeat("a", 55)+"x") == CRCAgentAccountName(strings.Repeat("a", 55)+"y") {
		t.Fatal("truncated names collide")
	}
}
