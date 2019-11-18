#!/usr/bin

KUBE_GIT_COMMIT=`git rev-parse "HEAD^{commit}"`
KUBE_GIT_VERSION=`git describe --tags --abbrev=14 "${KUBE_GIT_COMMIT}^{commit}" --always`
KUBE_GO_PACKAGE=''
ldflags() {
  local -a ldflags
  function add_ldflag() {
    local key=${1}
    local val=${2}
    ldflags+=(
      "-X '${KUBE_GO_PACKAGE}k8s.io/kubernetes/pkg/version.${key}=${val}'"
    )
  }

  add_ldflag "buildDate" "$(date ${SOURCE_DATE_EPOCH:+"--date=@${SOURCE_DATE_EPOCH}"} -u +'%Y-%m-%dT%H:%M:%SZ')"
  if [[ -n ${KUBE_GIT_COMMIT-} ]]; then
    add_ldflag "gitCommit" "${KUBE_GIT_COMMIT}"
    add_ldflag "gitTreeState" "${KUBE_GIT_TREE_STATE}"
  fi

  if [[ -n ${KUBE_GIT_VERSION-} ]]; then
    add_ldflag "gitVersion" "${KUBE_GIT_VERSION}"
  fi

  if [[ -n ${KUBE_GIT_MAJOR-} && -n ${KUBE_GIT_MINOR-} ]]; then
    add_ldflag "gitMajor" "${KUBE_GIT_MAJOR}"
    add_ldflag "gitMinor" "${KUBE_GIT_MINOR}"
  fi

  # The -ldflags parameter takes a single string, so join the output.
  echo "${ldflags[*]-}"
}

ldflags
