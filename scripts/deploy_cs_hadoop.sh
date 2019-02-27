#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh

ROOT_DIR=~

work() {
    local node=$1
    ssh ${node} "wget -N -P ${ROOT_DIR} ${RELEASE_REPO_URL}/libs/stargate-commons-1.0.jar" < /dev/null
    ssh ${node} "wget -N -P ${ROOT_DIR} ${RELEASE_REPO_URL}/libs/stargate-hdfs-client-1.0.jar" < /dev/null
    echo "Done ${node}"
}

while read node; do
    echo "connecting to ${node}"
    work ${node} &
done <cs_hadoop_cluster.txt
