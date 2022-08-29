#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")
PARENT_PARENT_PATH=$(dirname "${PARENT_PATH}")

source "${PARENT_PATH}/common.sh"

config_files=("${PARENT_PATH}/config.properties")

data_path="${SCRIPT_PATH}"
cd "${data_path}"

spark_home=$(prop ${config_files} 'spark.home')
spark_submit="${spark_home}/bin/spark-submit"
spark_master=$(prop ${config_files} 'spark.master.url')
cqc_home="${PARENT_PARENT_PATH}"
cqc_jar="${cqc_home}/target/ComparisonJoins-1.0-SNAPSHOT.jar"
class_name="org.SparkCQC.GraphPreparation"
cores_max="16"
default_parallelism="32"
driver_memory="16G"
executor_cores="16"
executor_memory="360G"
main_args1="${data_path}"

rm -rf "${data_path}/*-prepared"

log_file_path="${PARENT_PATH}/log"
mkdir -p "${log_file_path}"

log_file="${log_file_path}/prepare_graph_data.log"
touch "${log_file}"

${spark_submit} "--class" "${class_name}" "--master" "${spark_master}" \
"--conf" "spark.cores.max=${cores_max}" "--conf" "spark.default.parallelism=${default_parallelism}" \
"--conf" "spark.driver.memory=${driver_memory}" "--conf" "spark.executor.cores=${executor_cores}" \
"--conf" "spark.executor.memory=${executor_memory}" \
"${cqc_jar}" "${main_args1}" >> ${log_file} 2>&1