package controller

import (
	"os"
	"path/filepath"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

var _ = Describe("CRC agent installed authorization", func() {
	It("lets the manager bind the installed role and limits each agent to its namespace", func() {
		const (
			managerUserName = "test-agent-rbac-manager"
			vmiGroup        = "kubevirt.io"
			vmiResource     = "virtualmachineinstances"
			secretResource  = "secrets"
		)
		previous, set := os.LookupEnv(resources.CRCAgentClusterRoleEnvVar)
		Expect(os.Setenv(resources.CRCAgentClusterRoleEnvVar, "guestcluster-operator-crc-agent-instance-role")).To(Succeed())
		DeferCleanup(func() {
			if set {
				_ = os.Setenv(resources.CRCAgentClusterRoleEnvVar, previous)
			} else {
				_ = os.Unsetenv(resources.CRCAgentClusterRoleEnvVar)
			}
		})
		managerBytes, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "role.yaml"))
		Expect(err).NotTo(HaveOccurred())
		agentBytes, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "crc_agent_instance_role.yaml"))
		Expect(err).NotTo(HaveOccurred())
		managerRole := &rbacv1.ClusterRole{}
		agentRole := &rbacv1.ClusterRole{}
		Expect(yaml.Unmarshal(managerBytes, managerRole)).To(Succeed())
		Expect(yaml.Unmarshal(agentBytes, agentRole)).To(Succeed())
		managerRole.Name = managerUserName
		agentRole.Name = resources.CRCAgentClusterRole()
		Expect(k8sClient.Create(ctx, managerRole)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, managerRole); _ = k8sClient.Delete(ctx, agentRole) })
		managerBinding := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: managerUserName}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: managerRole.Name}, Subjects: []rbacv1.Subject{{Kind: rbacv1.UserKind, Name: managerUserName}}}
		Expect(k8sClient.Create(ctx, managerBinding)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, managerBinding) })
		one := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "agent-rbac-one"}}
		two := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "agent-rbac-two"}}
		Expect(k8sClient.Create(ctx, one)).To(Succeed())
		Expect(k8sClient.Create(ctx, two)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, one); _ = k8sClient.Delete(ctx, two) })
		managerConfig := rest.CopyConfig(cfg)
		managerConfig.Impersonate.UserName = managerUserName
		managerClient, err := client.New(managerConfig, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		instance := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "rbac-guest", Namespace: one.Name}, Spec: brokerv1alpha1.ClusterInstanceSpec{Type: brokerv1alpha1.TopologyCRC, Template: brokerv1alpha1.ClusterTemplate{Cores: 1, Memory: testMemory}}}
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, instance) })
		reconciler := &ClusterInstanceReconciler{Client: managerClient, Scheme: managerClient.Scheme()}
		Expect(reconciler.ensureCRCAgentRBAC(ctx, instance)).To(Succeed())
		installedRole := &rbacv1.ClusterRole{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(agentRole), installedRole)).To(Succeed())
		Expect(installedRole.Rules).To(Equal(agentRole.Rules))
		// This create call also verifies RBAC privilege escalation against the
		// manager's installed rules, without a broad bind permission.
		account := resources.CRCAgentAccountName(instance.Name)
		agentConfig := rest.CopyConfig(cfg)
		agentConfig.Impersonate.UserName = "system:serviceaccount:" + one.Name + ":" + account
		agentConfig.Impersonate.Groups = []string{"system:serviceaccounts", "system:serviceaccounts:" + one.Name, "system:authenticated"}
		typed, err := kubernetes.NewForConfig(agentConfig)
		Expect(err).NotTo(HaveOccurred())
		for _, check := range []struct{ group, resource, verb string }{
			{vmiGroup, vmiResource, "get"},
			{vmiGroup, vmiResource, "list"},
			{vmiGroup, vmiResource, "watch"},
			{"", secretResource, "get"}, {"", secretResource, "create"}, {"", secretResource, "update"},
		} {
			for _, namespace := range []string{one.Name, two.Name} {
				review, err := typed.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authv1.SelfSubjectAccessReview{Spec: authv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authv1.ResourceAttributes{Namespace: namespace, Group: check.group, Resource: check.resource, Verb: check.verb}}}, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
				Expect(review.Status.Allowed).To(Equal(namespace == one.Name), "authorization for %s %s in %s: %+v", check.verb, check.resource, namespace, review.Status)
			}
		}
	})
})
