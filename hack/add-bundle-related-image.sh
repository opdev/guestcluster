#!/usr/bin/env bash

set -euo pipefail

kustomize=$1
bundle_csv=$2
image=$3
use_image_digests=$4

tmpdir=$(mktemp -d)
trap 'rm -rf "${tmpdir}"' EXIT

cp "${bundle_csv}" "${tmpdir}/csv.yaml"
if [[ "${use_image_digests}" == true ]]; then
  printf '%s\n' \
    '- op: add' \
    '  path: /spec/relatedImages/-' \
    '  value:' \
    '    name: crc-agent' \
    "    image: ${image}" > "${tmpdir}/related-image-patch.yaml"
else
  printf '%s\n' \
    '- op: add' \
    '  path: /spec/relatedImages' \
    '  value:' \
    '  - name: crc-agent' \
    "    image: ${image}" > "${tmpdir}/related-image-patch.yaml"
fi

(
  cd "${tmpdir}"
  "${kustomize}" create --resources csv.yaml
  "${kustomize}" edit add patch \
    --path related-image-patch.yaml \
    --group operators.coreos.com \
    --version v1alpha1 \
    --kind ClusterServiceVersion
)
"${kustomize}" build "${tmpdir}" > "${bundle_csv}.tmp"
mv "${bundle_csv}.tmp" "${bundle_csv}"
