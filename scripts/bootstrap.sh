#! /bin/bash
# current directory
SCRIPTDIR=$(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}"))
BASEDIR=$(dirname $(dirname ${SCRIPTDIR}))
JARDIR="${BASEDIR}/target"
DEPDIR="${BASEDIR}/target/dependency"

RELDIR="${BASEDIR}/release"
RELLIBDIR="${RELDIR}/libs"

RELEASE_NAME=stargate-hdfs-client-release-1.0
RELEASE_ARCHIVE_FILENAME=${RELEASE_NAME}.tar.gz

RELEASE_REPO_URL=https://butler.opencloud.cs.arizona.edu/demo_apps/stargate
