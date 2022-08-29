#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")

tpc_e_path="${SCRIPT_PATH}"
cd "${tpc_e_path}/tpc_e_tool"

./bin/EGenLoader