#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")

tpc_e_path="${SCRIPT_PATH}"
cd "${tpc_e_path}"

if [[ ! -f "tpc-e-tool.zip" ]]; then
    echo "tpc-e-tool.zip not exist."
    echo "please download tpc-e-tool.zip from https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp"
    exit 1
fi

rm -rf "./tpc-e-tool"
mkdir tpc_e_tool

cp tpc-e-tool.zip tpc_e_tool/
cd tpc_e_tool

unzip tpc-e-tool.zip > /dev/null 2>&1
rm -f tpc-e-tool.zip
cd prj/
make > /dev/null 2>&1

cd ..
if [[ -f "bin/EGenLoader" ]]; then
    echo "tpc_e_tool build success"
else
    echo "tpc_e_tool build fail"
fi