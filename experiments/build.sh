#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

source "${SCRIPT_PATH}/common.sh"

config_files=("${SCRIPT_PATH}/config.properties")

log_file="${SCRIPT_PATH}/log/build.log"

mkdir -p "${SCRIPT_PATH}/log"
rm -f ${log_file}
touch ${log_file}

spark_cqc_home="${PARENT_PATH}"
cd ${spark_cqc_home}

rm -rf "${spark_cqc_home}/target/"

mvn "clean" >> ${log_file} 2>&1

mvn "compile" "-DskipTests=true" >> ${log_file} 2>&1
assert "compile failed."

mvn "package" "-DskipTests=true" >> ${log_file} 2>&1
assert "package failed."
