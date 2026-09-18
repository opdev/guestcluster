#!/usr/bin/env bash

set -euo pipefail

usage() {
  printf '%s\n' \
    "usage: $0 build-installer KUSTOMIZE IMAGE CRC_AGENT_IMAGE OUTPUT" \
    "       $0 deploy KUSTOMIZE KUBECTL IMAGE CRC_AGENT_IMAGE" \
    "       $0 undeploy KUSTOMIZE KUBECTL IMAGE CRC_AGENT_IMAGE IGNORE_NOT_FOUND" >&2
  exit 2
}

[[ $# -ge 1 ]] || usage

mode=$1
case "${mode}" in
  build-installer)
    [[ $# -eq 5 ]] || usage
    kustomize=$2
    image=$3
    crc_agent_image=$4
    output=$5
    ;;
  deploy)
    [[ $# -eq 5 ]] || usage
    kustomize=$2
    kubectl=$3
    image=$4
    crc_agent_image=$5
    ;;
  undeploy)
    [[ $# -eq 6 ]] || usage
    kustomize=$2
    kubectl=$3
    image=$4
    crc_agent_image=$5
    ignore_not_found=$6
    ;;
  *)
    usage
    ;;
esac

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [[ "${kustomize}" != */* ]]; then
  kustomize=$(command -v "${kustomize}")
else
  kustomize=$(cd "$(dirname "${kustomize}")" && pwd)/$(basename "${kustomize}")
fi
tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/guestcluster-deployment.XXXXXX")
trap 'rm -rf -- "${tmpdir}"' EXIT

cp -R "${repo_root}/config" "${tmpdir}/config"

# Change only the copied kustomization. The repository's manager image remains
# unchanged even when rendering or applying the deployment fails.
(
  cd "${tmpdir}/config/manager"
  "${kustomize}" edit set image "controller=${image}"
)

# Keep direct deployment's CRC agent configuration identical to the bundle
# path. The source manager manifest intentionally leaves this production image
# unset, so add the value only to the temporary rendering copy.
printf '%s\n' \
  'apiVersion: apps/v1' \
  'kind: Deployment' \
  'metadata:' \
  '  name: controller-manager' \
  'spec:' \
  '  template:' \
  '    spec:' \
  '      containers:' \
  '      - name: manager' \
  '        env:' \
  '        - name: CRC_AGENT_IMAGE' \
  "          value: ${crc_agent_image}" > "${tmpdir}/config/default/crc-agent-image-patch.yaml"
(
  cd "${tmpdir}/config/default"
  "${kustomize}" edit add patch \
    --path crc-agent-image-patch.yaml \
    --group apps \
    --version v1 \
    --kind Deployment \
    --name controller-manager
)

default_manifest="${tmpdir}/default.yaml"

# Render the complete direct-deployment manifest before applying or deleting
# any resource. This avoids starting a cluster operation with partial output.
"${kustomize}" build "${tmpdir}/config/default" > "${default_manifest}"

case "${mode}" in
  build-installer)
    output_dir=$(dirname "${output}")
    mkdir -p "${output_dir}"
    output_tmp=$(mktemp "${output}.tmp.XXXXXX")
    trap 'rm -rf -- "${tmpdir}" "${output_tmp:-}"' EXIT
    cat "${default_manifest}" > "${output_tmp}"
    mv "${output_tmp}" "${output}"
    ;;
  deploy)
    "${kubectl}" apply -f "${default_manifest}"
    ;;
  undeploy)
    "${kubectl}" delete "--ignore-not-found=${ignore_not_found}" -f "${default_manifest}"
    ;;
esac
