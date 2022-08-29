#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PLOT_PATH=$(dirname "${SCRIPT_PATH}")
ROOT_PATH=$(dirname "${PLOT_PATH}")

function load {
    avg_file=$1

    if [[ ! -f ${avg_file} ]]; then
        echo "-1"
    else
        content=$(cat "${avg_file}")
        if [[ -n ${content} ]]; then
            echo "${content}"
        else
            echo "-1"
        fi
    fi
}

function append {
    io_avg_file=$1
    sparkcqc_avg_file=$2
    sparksql_avg_file=$3
    postgresql_avg_file=$4
    willard_avg_file=$5
    ank_k_avg_file=$6
    target_file=$7

    io_avg=$(load "${io_avg_file}")
    sparkcqc_avg=$(load "${sparkcqc_avg_file}")
    sparksql_avg=$(load "${sparksql_avg_file}")
    postgresql_avg=$(load "${postgresql_avg_file}")
    willard_avg=$(load "${willard_avg_file}")
    ank_k_avg=$(load "${ank_k_avg_file}")

    echo "${io_avg} ${sparkcqc_avg} ${sparksql_avg} ${postgresql_avg} ${willard_avg} ${ank_k_avg}" >> "${target_file}"
}

target_file_path="${ROOT_PATH}/output/result/different_systems"
target_file="${target_file_path}/result.dat"
mkdir -p "${target_file_path}"
rm -f "${target_file}"
touch "${target_file}"

result_path="${ROOT_PATH}/output/result/different_systems"
bash "${PLOT_PATH}/compute_average.sh" "${result_path}"
append "${result_path}/task1.avg" "${result_path}/task2.avg" "${result_path}/task3.avg" "${result_path}/task4.avg" "${result_path}/task5.avg" "${result_path}/task6.avg" "${target_file}"

cd "${ROOT_PATH}"
mkdir -p "output/figure/different_systems"
gnuplot -c "plot/different_systems/plot.plt"