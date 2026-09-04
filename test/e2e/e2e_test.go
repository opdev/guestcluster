/*
Copyright 2026.

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

package e2e

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/caxu-rh/guestcluster-operator/test/utils"
)

// namespace where the project is deployed in
const namespace = "guestcluster-operator-system"

// serviceAccountName created for the project
const serviceAccountName = "guestcluster-operator-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "guestcluster-operator-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "guestcluster-operator-metrics-binding"

// openshiftConfigNamespace is the namespace that holds the management
// cluster's own global pull secret. On a real OpenShift cluster this
// namespace always exists; on a plain Kind cluster (used by this e2e suite)
// it does not, so `make deploy` fails when it applies the RBAC objects in
// config/openshift-config-rbac. See ClusterInstanceReconciler.resolvePullSecret.
const openshiftConfigNamespace = "openshift-config"

const crcRecoveryNamespace = "crc-recovery-e2e"

const crcRecoveryInstanceName = "crc-vmi-recovery"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// openshiftConfigNamespaceCreated tracks whether this suite created the
	// openshift-config namespace, so AfterAll only removes it if the suite
	// is the one that added it (e.g. it must be left alone on real OpenShift
	// clusters where it pre-exists).
	var openshiftConfigNamespaceCreated bool

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("ensuring the openshift-config namespace exists")
		cmd = exec.Command("kubectl", "get", "ns", openshiftConfigNamespace)
		if _, err = utils.Run(cmd); err != nil {
			cmd = exec.Command("kubectl", "create", "ns", openshiftConfigNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create openshift-config namespace")
			openshiftConfigNamespaceCreated = true
		}

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("installing synthetic CRC backing CRDs for recovery tests")
		cmd = exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/vmi-crd.yaml")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install VirtualMachineInstance CRD")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	})

	// Remove custom resources while the controller is still running. CRD deletion
	// waits for custom-resource finalizers.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("removing the CRC recovery instance before undeploying the controller-manager")
		cmd = exec.Command("kubectl", "delete", "clusterinstance", crcRecoveryInstanceName,
			"-n", crcRecoveryNamespace, "--ignore-not-found", "--wait=true", "--timeout=2m")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to remove CRC recovery instance")

		By("removing the CRC recovery namespace before uninstalling CRDs")
		cmd = exec.Command("kubectl", "delete", "namespace", crcRecoveryNamespace,
			"--ignore-not-found", "--wait=true", "--timeout=2m")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to remove CRC recovery namespace")

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing synthetic CRC backing CRDs")
		cmd = exec.Command("kubectl", "delete", "-f", "test/e2e/testdata/vmi-crd.yaml", "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)

		if openshiftConfigNamespaceCreated {
			By("removing openshift-config namespace")
			cmd = exec.Command("kubectl", "delete", "ns", openshiftConfigNamespace)
			_, _ = utils.Run(cmd)
		}
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			// The recovery test restarts the controller, so refresh its pod name
			// before collecting diagnostics.
			cmd := exec.Command("kubectl", "get", "pods", "-l", "control-plane=controller-manager",
				"-o", "go-template={{ range .items }}{{ if not .metadata.deletionTimestamp }}"+
					"{{ .metadata.name }}{{ \"\\n\" }}{{ end }}{{ end }}",
				"-n", namespace)
			if podOutput, err := utils.Run(cmd); err == nil {
				podNames := utils.GetNonEmptyLines(podOutput)
				if len(podNames) == 1 {
					controllerPodName = podNames[0]
				}
			}

			By("Fetching controller manager pod logs")
			cmd = exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				// Get the name of the controller-manager pod
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				// Validate the pod's status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=guestcluster-operator-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("waiting for the metrics endpoint to be ready")
			verifyMetricsEndpointReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "endpoints", metricsServiceName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("8443"), "Metrics endpoint is not ready")
			}
			Eventually(verifyMetricsEndpointReady).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("controller-runtime.metrics\tServing metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted).Should(Succeed())

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": ["curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics"],
							"securityContext": {
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccount": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			metricsOutput := getMetricsOutput()
			Expect(metricsOutput).To(ContainSubstring(
				"controller_runtime_reconcile_total",
			))
		})

		It("should invalidate CRC handoff after VMI replacement", func() {
			By("creating an isolated namespace and readiness identity")
			cmd := exec.Command("kubectl", "create", "namespace", crcRecoveryNamespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			cmd = exec.Command("kubectl", "create", "serviceaccount", "crc-readyz", "-n", crcRecoveryNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			cmd = exec.Command("kubectl", "create", "clusterrole", "crc-readyz", "--verb=get", "--non-resource-url=/readyz")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(deleteResource, "clusterrole", "crc-readyz")
			cmd = exec.Command("kubectl", "create", "clusterrolebinding", "crc-readyz",
				"--clusterrole=crc-readyz", fmt.Sprintf("--serviceaccount=%s:crc-readyz", crcRecoveryNamespace))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(deleteResource, "clusterrolebinding", "crc-readyz")
			cmd = exec.Command("kubectl", "create", "token", "crc-readyz", "-n", crcRecoveryNamespace)
			token, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("stopping the controller while creating the synthetic Ready CRC instance")
			cmd = exec.Command("kubectl", "scale", "deployment",
				"guestcluster-operator-controller-manager", "-n", namespace, "--replicas=0")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			cmd = exec.Command("kubectl", "rollout", "status",
				"deployment/guestcluster-operator-controller-manager", "-n", namespace, "--timeout=2m")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				cmd := exec.Command("kubectl", "scale", "deployment",
					"guestcluster-operator-controller-manager", "-n", namespace, "--replicas=1")
				_, _ = utils.Run(cmd)
			})

			By("creating a Ready CRC instance with its first VMI")
			instanceName := crcRecoveryInstanceName
			Expect(applyManifest(fmt.Sprintf(`
apiVersion: kubevirt.io/v1
kind: VirtualMachineInstance
metadata:
  name: %[1]s
  namespace: %[2]s
spec: {}
---
apiVersion: v1
kind: Secret
metadata:
  name: %[1]s-pull-secret
  namespace: %[2]s
type: kubernetes.io/dockerconfigjson
data:
  .dockerconfigjson: e30=
---
apiVersion: v1
kind: Secret
metadata:
  name: %[1]s-bundle-ssh-key
  namespace: %[2]s
data:
  id_ecdsa: dGVzdA==
---
apiVersion: v1
kind: Secret
metadata:
  name: %[1]s-kubeconfig
  namespace: %[2]s
data:
  kubeconfig: %s
---
apiVersion: guestcluster.opdev.io/v1alpha1
kind: ClusterInstance
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  type: crc
  template:
    ocpVersion: "4.16.0"
    memory: 16Gi
    cores: 4
    rootVolumeSize: 80Gi
    releaseImage: https://example.test/crc.qcow2
    pullSecretRef:
      name: %[1]s-pull-secret
    bundleSSHKeyRef:
      name: %[1]s-bundle-ssh-key
`, instanceName, crcRecoveryNamespace,
				base64.StdEncoding.EncodeToString([]byte(readyzKubeconfig(token)))))).To(Succeed())

			oldVMIUID := resourceField("virtualmachineinstance", instanceName, "{.metadata.uid}")
			Expect(oldVMIUID).NotTo(BeEmpty())
			vmiStatus := `{"status":{"phase":"Running"}}`
			cmd = exec.Command("kubectl", "patch", "virtualmachineinstance", instanceName, "-n",
				crcRecoveryNamespace, "--subresource=status", "--type=merge", "-p", vmiStatus)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			status := fmt.Sprintf(`{"status":{"phase":"Ready","apiEndpoint":"https://kubernetes.default.svc",
"kubeconfigSecretRef":{"name":"%[1]s-kubeconfig"},"crc":{"vmName":"%[1]s",
"dataVolumeName":"%[1]s-rootdisk","vmiUID":"%[2]s"}}}`, instanceName, oldVMIUID)
			cmd = exec.Command("kubectl", "patch", "clusterinstance", instanceName, "-n",
				crcRecoveryNamespace, "--subresource=status", "--type=merge", "-p", status)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("resuming the controller")
			cmd = exec.Command("kubectl", "scale", "deployment",
				"guestcluster-operator-controller-manager", "-n", namespace, "--replicas=1")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			cmd = exec.Command("kubectl", "rollout", "status",
				"deployment/guestcluster-operator-controller-manager", "-n", namespace, "--timeout=2m")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("replacing the VMI")
			cmd = exec.Command("kubectl", "delete", "virtualmachineinstance", instanceName,
				"-n", crcRecoveryNamespace, "--wait=true")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("creating the replacement VMI")
			manifest := fmt.Sprintf("apiVersion: kubevirt.io/v1\nkind: VirtualMachineInstance\nmetadata:\n"+
				"  name: %s\n  namespace: %s\nspec: {}\n", instanceName, crcRecoveryNamespace)
			Expect(applyManifest(manifest)).To(Succeed())
			Expect(resourceField("virtualmachineinstance", instanceName, "{.metadata.uid}")).
				NotTo(Equal(oldVMIUID))
			cmd = exec.Command("kubectl", "patch", "virtualmachineinstance", instanceName, "-n",
				crcRecoveryNamespace, "--subresource=status", "--type=merge", "-p", vmiStatus)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for stale CRC handoff state to be removed")
			Eventually(func(g Gomega) {
				g.Expect(resourceField("clusterinstance", instanceName, "{.status.phase}")).
					To(Equal("Provisioning"))
				g.Expect(resourceField("clusterinstance", instanceName, "{.status.kubeconfigSecretRef.name}")).
					To(BeEmpty())
				g.Expect(resourceExists("secret", instanceName+"-kubeconfig", crcRecoveryNamespace)).To(BeFalse())
			}).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		// TODO: Customize the e2e test suite with scenarios specific to your project.
		// Consider applying sample/CR(s) and check their status and/or verifying
		// the reconciliation by using the metrics, i.e.:
		// metricsOutput := getMetricsOutput()
		// Expect(metricsOutput).To(ContainSubstring(
		//    fmt.Sprintf(`controller_runtime_reconcile_total{controller="%s",result="success"} 1`,
		//    strings.ToLower(<Kind>),
		// ))
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	// Temporary file to store the token request
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		// Execute kubectl command to create the token
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		// Parse the JSON output to extract the token
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() string {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	metricsOutput, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
	Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
	return metricsOutput
}

func applyManifest(manifest string) error {
	file, err := os.CreateTemp("", "guestcluster-e2e-*.yaml")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(file.Name()) }()
	if _, err := file.WriteString(manifest); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	_, err = utils.Run(exec.Command("kubectl", "apply", "-f", file.Name()))
	return err
}

func resourceField(resource, name, jsonPath string) string {
	cmd := exec.Command("kubectl", "get", resource, name, "-n", crcRecoveryNamespace, "-o", "jsonpath="+jsonPath)
	output, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return output
}

func resourceExists(resource, name, namespace string) bool {
	cmd := exec.Command("kubectl", "get", resource, name, "-n", namespace)
	_, err := utils.Run(cmd)
	return err == nil
}

func deleteResource(resource, name string) {
	_, _ = utils.Run(exec.Command("kubectl", "delete", resource, name, "--ignore-not-found"))
}

func readyzKubeconfig(token string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://kubernetes.default.svc
    insecure-skip-tls-verify: true
  name: management
contexts:
- context:
    cluster: management
    user: readyz
  name: management
current-context: management
users:
- name: readyz
  user:
    token: %s
`, token)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
