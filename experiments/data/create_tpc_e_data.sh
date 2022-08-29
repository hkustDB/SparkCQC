#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")
PARENT_PARENT_PATH=$(dirname "${PARENT_PATH}")

source "${PARENT_PATH}/common.sh"

config_files=("${PARENT_PATH}/config.properties")

data_path="${SCRIPT_PATH}"
tpc_e_path="${PARENT_PATH}/tpc_e"

log_file_path="${PARENT_PATH}/log"
mkdir -p "${log_file_path}"

log_file="${log_file_path}/create_tpc_e_data.log"
touch "${log_file}"

rm -f "${data_path}/trade.txt"
rm -f "${data_path}/tradeB.txt"
rm -f "${data_path}/tradeS.txt"
rm -f "${data_path}/trade.in"

scala -J-Xmx360g -J-Xmx360g -cp "${PARENT_PARENT_PATH}/target/ComparisonJoins-1.0-SNAPSHOT.jar" "org.SparkCQC.TradeToHold" "${tpc_e_path}/tpc_e_tool/flat_out/Trade.txt" "${data_path}" >> "${log_file}" 2>&1
assert "tpc-e data conversion failed. Please check ${log_file} for more details."