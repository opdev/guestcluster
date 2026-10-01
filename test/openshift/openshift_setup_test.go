//go:build openshift

package openshift

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	directManagerNamespace = "gc-openshift-e2e-manager"
	directManagerPrefix    = "gc-openshift-e2e-"
	directBundleNamespace  = defaultOperatorNamespace
	managerImageRepository = "guestcluster-operator-controller"
	agentImageRepository   = "guestcluster-operator-crc-agent"
	imageBuilderAccount    = "default"
	kustomizeEdit          = "edit"
	kustomizeSet           = "set"
)

type directManagerInstall struct {
	namespace       string
	deployment      string
	managerImage    string
	agentImage      string
	bundleNamespace string
	inputNamespace  string
	imageNamespace  string
	imagePullSecret string
}

type suspendedManager struct {
	namespace string
	name      string
	replicas  int32
}

func installDirectManager(t *testing.T, kube kubernetes.Interface, config *rest.Config) directManagerInstall {
	t.Helper()
	bundleNamespace := envOrDefault("OPENSHIFT_BUNDLE_NAMESPACE", directBundleNamespace)
	inputNamespace := envOrDefault("OPENSHIFT_INPUT_NAMESPACE", bundleNamespace)
	imageNamespace := directManagerNamespace
	install := directManagerInstall{
		namespace:       directManagerNamespace,
		deployment:      directManagerPrefix + "controller-manager",
		bundleNamespace: bundleNamespace,
		inputNamespace:  inputNamespace,
		imageNamespace:  imageNamespace,
	}

	repoRoot, err := findRepositoryRoot()
	must(t, err)
	kubectl := commandPath(t, "KUBECTL", "kubectl", repoRoot)
	kustomize := commandPath(t, "KUSTOMIZE", "kustomize", repoRoot)
	docker := commandPath(t, "DOCKER", "docker", repoRoot)
	skopeo := commandPath(t, "SKOPEO", "skopeo", repoRoot)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	dockerHost, err := dockerDaemonHost(ctx, docker)
	must(t, err)

	// Remove resources from an interrupted earlier OpenShift test before setup.
	if err := cleanupDirectManagerResources(ctx, kube, bundleNamespace, imageNamespace); err != nil {
		t.Fatalf("cleaning a stale direct operator install: %v", err)
	}
	if err := ensureNoExistingGuestClusters(ctx, config); err != nil {
		t.Fatal(err)
	}

	tag := "openshift-e2e"
	localManagerImage := "guestcluster-operator-controller:" + tag
	localAgentImage := "guestcluster-operator-crc-agent:" + tag
	if err := buildLocalImages(ctx, docker, repoRoot, localManagerImage, localAgentImage); err != nil {
		t.Fatalf("building the manager and CRC-agent images: %v", err)
	}

	var paused *suspendedManager
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cleanupCancel()
		if err := cleanupDirectManagerResources(cleanupCtx, kube, bundleNamespace, imageNamespace); err != nil {
			t.Errorf("cleaning direct operator test install: %v", err)
		}
		if paused != nil {
			if err := restoreInstalledManager(kube, paused); err != nil {
				t.Errorf("restoring manager Deployment %s/%s: %v", paused.namespace, paused.name, err)
			}
		}
	})

	if _, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: install.namespace},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating direct manager namespace %s: %v", install.namespace, err)
	}
	if err := createImageBuilderResources(ctx, kube, config, install.namespace); err != nil {
		t.Fatalf("creating test image streams and push credentials: %v", err)
	}
	managerImage, agentImage, imagePullSecret, err := publishTestImages(
		ctx, kube, config, skopeo, dockerHost, install.namespace, localManagerImage, localAgentImage, tag,
	)
	if err != nil {
		t.Fatalf("pushing the manager and CRC-agent images to the test registry: %v", err)
	}
	install.managerImage = managerImage
	install.agentImage = agentImage
	install.imagePullSecret = imagePullSecret
	if err := createDirectManagerBindings(ctx, kube, install); err != nil {
		t.Fatalf("creating direct manager image and CDI permissions: %v", err)
	}
	paused, err = pauseInstalledManager(ctx, kube, defaultOperatorNamespace)
	if err != nil {
		t.Fatalf("pausing the existing manager before the direct install: %v", err)
	}

	manifest, err := renderDirectManager(
		ctx, repoRoot, kubectl, kustomize, managerImage, agentImage, bundleNamespace, imagePullSecret,
	)
	must(t, err)
	if _, err := runCommand(ctx, kubectl, []string{"apply", "-f", "-"}, manifest); err != nil {
		t.Fatalf("applying direct manager manifests: %v", err)
	}
	if _, err := runCommand(ctx, kubectl, []string{
		"wait", "--for=condition=Established", "--timeout=2m",
		"crd/clusterinstances.guestcluster.opdev.io",
		"crd/clusterleases.guestcluster.opdev.io",
		"crd/clusterpools.guestcluster.opdev.io",
		"crd/crcbundles.guestcluster.opdev.io",
	}, nil); err != nil {
		t.Fatalf("waiting for operator CRDs: %v", err)
	}
	deploymentCtx, deploymentCancel := context.WithTimeout(ctx, 10*time.Minute)
	defer deploymentCancel()
	if err := waitForDeployment(deploymentCtx, kube, install.namespace, install.deployment, 1); err != nil {
		t.Fatalf("waiting for direct manager Deployment: %v", err)
	}
	return install
}

func buildLocalImages(
	ctx context.Context,
	docker, repoRoot, managerImage, agentImage string,
) error {
	if _, err := runCommand(ctx, docker, []string{
		"build", "--platform=linux/amd64", "--tag", managerImage, "--file", "Dockerfile", ".",
	}, nil, repoRoot); err != nil {
		return fmt.Errorf("building manager image: %w", err)
	}
	if _, err := runCommand(ctx, docker, []string{
		"build", "--platform=linux/amd64", "--tag", agentImage, "--file", "Dockerfile.crc-agent", ".",
	}, nil, repoRoot); err != nil {
		return fmt.Errorf("building CRC-agent image: %w", err)
	}
	return nil
}

func dockerDaemonHost(ctx context.Context, docker string) (string, error) {
	if host := os.Getenv("DOCKER_HOST"); host != "" {
		return host, nil
	}
	output, err := runCommand(ctx, docker, []string{
		"context", "inspect", "--format", "{{.Endpoints.docker.Host}}",
	}, nil)
	if err != nil {
		return "", err
	}
	host := strings.TrimSpace(string(output))
	if host == "" {
		return "", fmt.Errorf("Docker context returned an empty daemon endpoint")
	}
	return host, nil
}

func createImageBuilderResources(
	ctx context.Context,
	kube kubernetes.Interface,
	config *rest.Config,
	namespace string,
) error {
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: imageBuilderAccount, Namespace: namespace},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: clusterRoleKind, Name: "system:image-builder"},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.ServiceAccountKind, Name: imageBuilderAccount, Namespace: namespace,
		}},
	}
	if _, err := kube.RbacV1().RoleBindings(namespace).Create(ctx, binding, metav1.CreateOptions{}); err != nil {
		return err
	}
	registryClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	imageStreams := registryClient.Resource(schema.GroupVersionResource{
		Group: "image.openshift.io", Version: "v1", Resource: "imagestreams",
	})
	for _, name := range []string{managerImageRepository, agentImageRepository} {
		stream := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "image.openshift.io/v1",
			"kind":       "ImageStream",
			"metadata":   map[string]interface{}{"name": name},
		}}
		if _, err := imageStreams.Namespace(namespace).Create(ctx, stream, metav1.CreateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func publishTestImages(
	ctx context.Context,
	kube kubernetes.Interface,
	config *rest.Config,
	skopeo, dockerHost, namespace, localManagerImage, localAgentImage, tag string,
) (managerImage, agentImage, imagePullSecret string, resultErr error) {
	registryClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return "", "", "", fmt.Errorf("creating registry client: %w", err)
	}
	registryConfig := registryClient.Resource(schema.GroupVersionResource{
		Group: "imageregistry.operator.openshift.io", Version: "v1", Resource: "configs",
	})
	clusterConfig, err := registryConfig.Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		return "", "", "", fmt.Errorf("getting ImageRegistry config: %w", err)
	}
	originalRoute, _, err := unstructured.NestedBool(clusterConfig.Object, "spec", "defaultRoute")
	if err != nil {
		return "", "", "", err
	}
	var routeHost string
	if originalRoute {
		routeHost, err = waitForRegistryRoute(ctx, registryClient, true)
	} else {
		defer func() {
			restoreCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			if _, restoreErr := setRegistryDefaultRoute(restoreCtx, registryClient, false); restoreErr != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("restoring image registry default route: %w", restoreErr))
			}
		}()
		routeHost, err = setRegistryDefaultRoute(ctx, registryClient, true)
	}
	if err != nil {
		return "", "", "", err
	}

	credentials, imagePullSecret, err := imageBuilderRegistryAuth(ctx, kube, namespace, routeHost)
	if err != nil {
		return "", "", "", err
	}

	authDirectory, err := os.MkdirTemp("", "guestcluster-openshift-registry-auth-")
	if err != nil {
		return "", "", "", err
	}
	defer func() {
		if cleanupErr := os.RemoveAll(authDirectory); resultErr == nil && cleanupErr != nil {
			resultErr = cleanupErr
		}
	}()
	authFile := filepath.Join(authDirectory, "auth.json")
	if err := os.WriteFile(authFile, credentials, 0o600); err != nil {
		return "", "", "", err
	}
	registriesConfig := filepath.Join(authDirectory, "registries.conf")
	if err := os.WriteFile(
		registriesConfig,
		[]byte("unqualified-search-registries = [\"docker.io\"]\n"),
		0o600,
	); err != nil {
		return "", "", "", err
	}

	for _, image := range []struct {
		local      string
		repository string
	}{
		{local: localManagerImage, repository: managerImageRepository},
		{local: localAgentImage, repository: agentImageRepository},
	} {
		destination := fmt.Sprintf("docker://%s/%s/%s:%s", routeHost, namespace, image.repository, tag)
		if _, err := runCommandWithEnv(ctx, skopeo, []string{
			"copy", "--src-daemon-host", dockerHost, "--authfile", authFile,
			"--dest-tls-verify=false", "--retry-times=3",
			"docker-daemon:" + image.local, destination,
		}, nil, "", []string{"CONTAINERS_REGISTRIES_CONF=" + registriesConfig}); err != nil {
			return "", "", "", fmt.Errorf("pushing image %s: %w", image.local, err)
		}
		if err := waitForImageStreamTag(ctx, registryClient, namespace, image.repository, tag); err != nil {
			return "", "", "", err
		}
	}

	managerImage = internalRegistryImage(namespace, managerImageRepository, tag)
	agentImage = internalRegistryImage(namespace, agentImageRepository, tag)
	return managerImage, agentImage, imagePullSecret, nil
}

func imageBuilderRegistryAuth(
	ctx context.Context,
	kube kubernetes.Interface,
	namespace, registryHost string,
) ([]byte, string, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		serviceAccount, err := kube.CoreV1().ServiceAccounts(namespace).Get(ctx, imageBuilderAccount, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, "", err
		}
		if err == nil {
			secretNames := make([]string, 0, len(serviceAccount.ImagePullSecrets)+len(serviceAccount.Secrets))
			for _, ref := range serviceAccount.ImagePullSecrets {
				secretNames = append(secretNames, ref.Name)
			}
			for _, ref := range serviceAccount.Secrets {
				secretNames = append(secretNames, ref.Name)
			}
			for _, name := range secretNames {
				secret, getErr := kube.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
				if apierrors.IsNotFound(getErr) {
					continue
				}
				if getErr != nil {
					return nil, "", getErr
				}
				if auth, ok := registryAuthForHost(secret, registryHost); ok {
					credentials, marshalErr := json.Marshal(map[string]interface{}{
						"auths": map[string]json.RawMessage{registryHost: auth},
					})
					return credentials, name, marshalErr
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil, "", fmt.Errorf("waiting for image-builder ServiceAccount registry credentials: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func registryAuthForHost(secret *corev1.Secret, registryHost string) (json.RawMessage, bool) {
	var config []byte
	switch secret.Type {
	case corev1.SecretTypeDockercfg:
		config = secret.Data[corev1.DockerConfigKey]
	case corev1.SecretTypeDockerConfigJson:
		config = secret.Data[corev1.DockerConfigJsonKey]
	default:
		return nil, false
	}
	if len(config) == 0 {
		return nil, false
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(config, &root); err != nil {
		return nil, false
	}
	entries := root
	if rawAuths, ok := root["auths"]; ok {
		entries = nil
		if err := json.Unmarshal(rawAuths, &entries); err != nil {
			return nil, false
		}
	}
	for _, host := range []string{
		registryHost,
		"image-registry.openshift-image-registry.svc:5000",
		"image-registry.openshift-image-registry.svc.cluster.local:5000",
	} {
		if entry, ok := entries[host]; ok {
			return entry, true
		}
	}
	for host, entry := range entries {
		if strings.Contains(host, "image-registry") && len(entry) > 0 {
			return entry, true
		}
	}
	return nil, false
}

func setRegistryDefaultRoute(ctx context.Context, registryClient dynamic.Interface, enabled bool) (string, error) {
	config := registryClient.Resource(schema.GroupVersionResource{
		Group: "imageregistry.operator.openshift.io", Version: "v1", Resource: "configs",
	})
	clusterConfig, err := config.Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("getting ImageRegistry config: %w", err)
	}
	spec, found, err := unstructured.NestedMap(clusterConfig.Object, "spec")
	if err != nil {
		return "", err
	}
	if !found {
		spec = map[string]interface{}{}
	}
	spec["defaultRoute"] = enabled
	if err := unstructured.SetNestedMap(clusterConfig.Object, spec, "spec"); err != nil {
		return "", err
	}
	if _, err := config.Update(ctx, clusterConfig, metav1.UpdateOptions{}); err != nil {
		return "", err
	}
	return waitForRegistryRoute(ctx, registryClient, enabled)
}

func waitForRegistryRoute(ctx context.Context, registryClient dynamic.Interface, enabled bool) (string, error) {
	routes := registryClient.Resource(schema.GroupVersionResource{
		Group: "route.openshift.io", Version: "v1", Resource: "routes",
	})
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		route, err := routes.Namespace("openshift-image-registry").Get(ctx, "default-route", metav1.GetOptions{})
		if !enabled && apierrors.IsNotFound(err) {
			return "", nil
		}
		if err == nil && enabled {
			host, _, hostErr := unstructured.NestedString(route.Object, "spec", "host")
			ingress, _, ingressErr := unstructured.NestedSlice(route.Object, "status", "ingress")
			if hostErr == nil && ingressErr == nil && host != "" && len(ingress) > 0 {
				return host, nil
			}
		} else if err != nil && !apierrors.IsNotFound(err) {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("waiting for image registry default route: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForImageStreamTag(
	ctx context.Context,
	registryClient dynamic.Interface,
	namespace, imageName, tag string,
) error {
	imageStreams := registryClient.Resource(schema.GroupVersionResource{
		Group: "image.openshift.io", Version: "v1", Resource: "imagestreams",
	})
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		stream, err := imageStreams.Namespace(namespace).Get(ctx, imageName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		tags, _, err := unstructured.NestedSlice(stream.Object, "status", "tags")
		if err != nil {
			return err
		}
		for _, entry := range tags {
			tagStatus, ok := entry.(map[string]interface{})
			if !ok {
				continue
			}
			statusTag, _, _ := unstructured.NestedString(tagStatus, "tag")
			items, _, _ := unstructured.NestedSlice(tagStatus, "items")
			if statusTag == tag && len(items) > 0 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for ImageStreamTag %s/%s:%s: %w", namespace, imageName, tag, ctx.Err())
		case <-ticker.C:
		}
	}
}

func internalRegistryImage(namespace, repository, tag string) string {
	return fmt.Sprintf("image-registry.openshift-image-registry.svc:5000/%s/%s:%s", namespace, repository, tag)
}

func ensureNoExistingGuestClusters(ctx context.Context, config *rest.Config) error {
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	resources := []struct {
		kind string
		gvr  schema.GroupVersionResource
	}{
		{
			kind: "ClusterInstance",
			gvr:  schema.GroupVersionResource{Group: "guestcluster.opdev.io", Version: "v1alpha1", Resource: "clusterinstances"},
		},
		{
			kind: "ClusterLease",
			gvr:  schema.GroupVersionResource{Group: "guestcluster.opdev.io", Version: "v1alpha1", Resource: "clusterleases"},
		},
	}
	var existing []string
	for _, resource := range resources {
		list, err := client.Resource(resource.gvr).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("checking for existing %s resources: %w", resource.kind, err)
		}
		for _, object := range list.Items {
			existing = append(existing, fmt.Sprintf("%s %s/%s", resource.kind, object.GetNamespace(), object.GetName()))
		}
	}
	if len(existing) > 0 {
		return fmt.Errorf(
			"the OpenShift test requires no existing guest instances or leases; found: %s",
			strings.Join(existing, ", "),
		)
	}
	return nil
}

func renderDirectManager(
	ctx context.Context,
	repoRoot, kubectl, kustomize, managerImage, agentImage, bundleNamespace, imagePullSecret string,
) (manifest []byte, resultErr error) {
	tmpDir, err := os.MkdirTemp("", "guestcluster-openshift-test-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if cleanupErr := os.RemoveAll(tmpDir); resultErr == nil && cleanupErr != nil {
			resultErr = cleanupErr
		}
	}()
	configDir := filepath.Join(tmpDir, "config")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.CopyFS(configDir, os.DirFS(filepath.Join(repoRoot, "config"))); err != nil {
		return nil, err
	}
	managerDir := filepath.Join(configDir, "manager")
	if _, err := runCommand(
		ctx, kustomize, []string{kustomizeEdit, kustomizeSet, "image", "controller=" + managerImage}, nil, managerDir,
	); err != nil {
		return nil, err
	}
	defaultDir := filepath.Join(configDir, "default")
	for _, args := range [][]string{
		{kustomizeEdit, kustomizeSet, "namespace", directManagerNamespace},
		{kustomizeEdit, kustomizeSet, "nameprefix", directManagerPrefix},
	} {
		if _, err := runCommand(ctx, kustomize, args, nil, defaultDir); err != nil {
			return nil, err
		}
	}
	patches := []struct {
		file string
		data string
	}{
		{
			file: "crc-agent-image-patch.yaml",
			data: fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: controller-manager
spec:
  template:
    spec:
      containers:
      - name: manager
        env:
        - name: CRC_AGENT_IMAGE
          value: %q
`, agentImage),
		},
		{
			file: "bundle-namespace-patch.yaml",
			data: fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: controller-manager
spec:
  template:
    spec:
      containers:
      - name: manager
        env:
        - name: OPERATOR_NAMESPACE
          valueFrom: null
          value: %q
`, bundleNamespace),
		},
		{
			file: "manager-image-pull-secret-patch.yaml",
			data: fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: controller-manager
spec:
  template:
    spec:
      imagePullSecrets:
      - name: %q
`, imagePullSecret),
		},
	}
	for _, patch := range patches {
		if err := os.WriteFile(filepath.Join(defaultDir, patch.file), []byte(patch.data), 0o600); err != nil {
			return nil, err
		}
		args := []string{
			kustomizeEdit, "add", "patch", "--path", patch.file,
			"--group", "apps", "--version", "v1", "--kind", "Deployment", "--name", "controller-manager",
		}
		if _, err := runCommand(ctx, kustomize, args, nil, defaultDir); err != nil {
			return nil, err
		}
	}
	return runCommand(ctx, kubectl, []string{"kustomize", defaultDir}, nil)
}

func createDirectManagerBindings(ctx context.Context, kube kubernetes.Interface, install directManagerInstall) error {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: directManagerPrefix + "cdi-clone-source", Namespace: install.bundleNamespace},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"cdi.kubevirt.io"},
			Resources: []string{"datavolumes/source"},
			Verbs:     []string{"create"},
		}},
	}
	if _, err := kube.RbacV1().Roles(install.bundleNamespace).Create(ctx, role, metav1.CreateOptions{}); err != nil {
		return err
	}
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: directManagerPrefix + "cdi-clone-source", Namespace: install.bundleNamespace},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: role.Name},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.ServiceAccountKind, Name: directManagerPrefix + "controller-manager", Namespace: install.namespace,
		}},
	}
	if _, err := kube.RbacV1().RoleBindings(install.bundleNamespace).Create(
		ctx, roleBinding, metav1.CreateOptions{},
	); err != nil {
		return err
	}
	imagePull := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: directManagerPrefix + "manager-image-pull", Namespace: install.imageNamespace},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: clusterRoleKind, Name: "system:image-puller"},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.GroupKind, Name: "system:serviceaccounts:" + install.namespace,
		}},
	}
	_, err := kube.RbacV1().RoleBindings(install.imageNamespace).Create(ctx, imagePull, metav1.CreateOptions{})
	return err
}

func pauseInstalledManager(
	ctx context.Context,
	kube kubernetes.Interface,
	namespace string,
) (*suspendedManager, error) {
	const deploymentName = "guestcluster-operator-controller-manager"
	deployment, err := kube.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	replicas := int32(1)
	if deployment.Spec.Replicas != nil {
		replicas = *deployment.Spec.Replicas
	}
	suspended := &suspendedManager{namespace: namespace, name: deploymentName, replicas: replicas}
	if replicas == 0 {
		return suspended, nil
	}
	zero := int32(0)
	deployment.Spec.Replicas = &zero
	if _, err := kube.AppsV1().Deployments(namespace).Update(
		ctx, deployment, metav1.UpdateOptions{},
	); err != nil {
		return nil, err
	}
	return suspended, waitForDeployment(ctx, kube, namespace, deploymentName, 0)
}

func restoreInstalledManager(kube kubernetes.Interface, suspended *suspendedManager) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	deployment, err := kube.AppsV1().Deployments(suspended.namespace).Get(ctx, suspended.name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	deployment.Spec.Replicas = &suspended.replicas
	if _, err := kube.AppsV1().Deployments(suspended.namespace).Update(
		ctx, deployment, metav1.UpdateOptions{},
	); err != nil {
		return err
	}
	return waitForDeployment(ctx, kube, suspended.namespace, suspended.name, suspended.replicas)
}

func waitForDeployment(ctx context.Context, kube kubernetes.Interface, namespace, name string, replicas int32) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		deployment, err := kube.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if replicas == 0 {
			if deployment.Status.Replicas == 0 && deployment.Status.ReadyReplicas == 0 {
				return nil
			}
		} else if deployment.Status.AvailableReplicas >= replicas && deployment.Status.UpdatedReplicas >= replicas {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for Deployment %s/%s replicas=%d: %w", namespace, name, replicas, ctx.Err())
		case <-ticker.C:
		}
	}
}

func cleanupStaleOpenShiftResources(ctx context.Context, kube kubernetes.Interface, imageNamespace string) error {
	var cleanupErrors []error
	namespaces, err := kube.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, namespace := range namespaces.Items {
		name := namespace.Name
		isOpenShiftTestNamespace := strings.HasPrefix(name, "gc-openshift-case-") ||
			strings.HasPrefix(name, "gc-openshift-rbac-")
		if isOpenShiftTestNamespace {
			if err := deleteNamespaceAndWait(ctx, kube, name); err != nil {
				cleanupErrors = append(cleanupErrors, err)
			}
		}
	}
	bindings, err := kube.RbacV1().RoleBindings(imageNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			cleanupErrors = append(cleanupErrors, err)
		}
		return errors.Join(cleanupErrors...)
	}
	for _, binding := range bindings.Items {
		if strings.HasPrefix(binding.Name, "guestcluster-pull-gc-openshift-case-") {
			if err := kube.RbacV1().RoleBindings(imageNamespace).Delete(
				ctx, binding.Name, metav1.DeleteOptions{},
			); err != nil && !apierrors.IsNotFound(err) {
				cleanupErrors = append(cleanupErrors, err)
			}
		}
	}
	return errors.Join(cleanupErrors...)
}

func cleanupDirectManagerResources(
	ctx context.Context,
	kube kubernetes.Interface,
	bundleNamespace, imageNamespace string,
) error {
	var cleanupErrors []error
	if err := cleanupStaleOpenShiftResources(ctx, kube, imageNamespace); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}
	cdiBinding := directManagerPrefix + "cdi-clone-source"
	if err := kube.RbacV1().RoleBindings(bundleNamespace).Delete(
		ctx, cdiBinding, metav1.DeleteOptions{},
	); err != nil && !apierrors.IsNotFound(err) {
		cleanupErrors = append(cleanupErrors, err)
	}
	if err := kube.RbacV1().Roles(bundleNamespace).Delete(
		ctx, cdiBinding, metav1.DeleteOptions{},
	); err != nil && !apierrors.IsNotFound(err) {
		cleanupErrors = append(cleanupErrors, err)
	}
	if err := kube.RbacV1().RoleBindings(imageNamespace).Delete(
		ctx, directManagerPrefix+"manager-image-pull", metav1.DeleteOptions{},
	); err != nil && !apierrors.IsNotFound(err) {
		cleanupErrors = append(cleanupErrors, err)
	}
	if err := deleteNamespaceAndWait(ctx, kube, directManagerNamespace); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}
	if err := cleanupPrefixedClusterRBAC(ctx, kube); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}
	return errors.Join(cleanupErrors...)
}

func cleanupPrefixedClusterRBAC(ctx context.Context, kube kubernetes.Interface) error {
	var cleanupErrors []error
	roles, err := kube.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	if err != nil {
		cleanupErrors = append(cleanupErrors, err)
	} else {
		for _, role := range roles.Items {
			if strings.HasPrefix(role.Name, directManagerPrefix) {
				if err := kube.RbacV1().ClusterRoles().Delete(
					ctx, role.Name, metav1.DeleteOptions{},
				); err != nil && !apierrors.IsNotFound(err) {
					cleanupErrors = append(cleanupErrors, err)
				}
			}
		}
	}
	bindings, err := kube.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		cleanupErrors = append(cleanupErrors, err)
	} else {
		for _, binding := range bindings.Items {
			if strings.HasPrefix(binding.Name, directManagerPrefix) {
				if err := kube.RbacV1().ClusterRoleBindings().Delete(
					ctx, binding.Name, metav1.DeleteOptions{},
				); err != nil && !apierrors.IsNotFound(err) {
					cleanupErrors = append(cleanupErrors, err)
				}
			}
		}
	}
	return errors.Join(cleanupErrors...)
}

func deleteNamespaceAndWait(ctx context.Context, kube kubernetes.Interface, name string) error {
	err := kube.CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		_, err := kube.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for Namespace %s deletion: %w", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

func findRepositoryRoot() (string, error) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for directory := workingDirectory; ; directory = filepath.Dir(directory) {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", fmt.Errorf("cannot locate repository root from %s", workingDirectory)
		}
	}
}

func commandPath(t *testing.T, envName, defaultName, repoRoot string) string {
	t.Helper()
	if value := os.Getenv(envName); value != "" {
		if !filepath.IsAbs(value) && strings.ContainsRune(value, os.PathSeparator) {
			absolutePath, err := filepath.Abs(filepath.Join(repoRoot, value))
			if err != nil {
				t.Fatalf("resolving %s path %q: %v", envName, value, err)
			}
			return absolutePath
		}
		return value
	}
	if defaultName == "kustomize" {
		candidate := filepath.Join(repoRoot, "bin", defaultName)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	path, err := exec.LookPath(defaultName)
	if err != nil {
		t.Fatalf("%s is required to install the OpenShift test manager: %v", defaultName, err)
	}
	return path
}

func runCommand(
	ctx context.Context,
	command string,
	args []string,
	stdin []byte,
	workingDirectory ...string,
) ([]byte, error) {
	workingDir := ""
	if len(workingDirectory) > 0 {
		workingDir = workingDirectory[0]
	}
	return runCommandWithEnv(ctx, command, args, stdin, workingDir, nil)
}

func runCommandWithEnv(
	ctx context.Context,
	command string,
	args []string,
	stdin []byte,
	workingDirectory string,
	extraEnv []string,
) ([]byte, error) {
	cmd := exec.CommandContext(ctx, command, args...)
	if workingDirectory != "" {
		cmd.Dir = workingDirectory
	}
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%s %s: %w: %s", command, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func envOrDefault(name, defaultValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defaultValue
}
