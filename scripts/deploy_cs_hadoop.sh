#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh

ROOT_DIR=~

work() {
    local node=$1
    ssh ${node} "wget --no-check-certificate -N -P ${ROOT_DIR} ${RELEASE_REPO_URL}/libs/stargate-commons-1.0.jar" < /dev/null
    ssh ${node} "wget --no-check-certificate -N -P ${ROOT_DIR} ${RELEASE_REPO_URL}/libs/stargate-hdfs-client-1.0.jar" < /dev/null
    echo "Done ${node}"
}

PIDS=()
while read node; do
    echo "connecting to ${node}"
    work ${node} &
    PIDS+=($!)
done <cs_hadoop_cluster.txt

for pid in ${PIDS[*]}; do
    wait $pid
done

echo "All jobs completed"
