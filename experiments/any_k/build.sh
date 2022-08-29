#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

source "${PARENT_PATH}/common.sh"

config_files=("${PARENT_PATH}/config.properties")

log_file="${PARENT_PATH}/log/build_anyk.log"

mkdir -p "${PARENT_PATH}/log"
rm -f ${log_file}
touch ${log_file}

cd "${SCRIPT_PATH}"
rm -rf "${SCRIPT_PATH}/anyk-code/"
git clone "git@github.com:northeastern-datalab/anyk-code.git" >> ${log_file} 2>&1

cp -f "${SCRIPT_PATH}/Path_Unranked.java" "${SCRIPT_PATH}/anyk-code/src/main/java/experiments/Path_Unranked.java"
cd "${SCRIPT_PATH}/anyk-code"

mvn package >> ${log_file} 2>&1