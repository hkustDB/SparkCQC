#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
SELECTIVITY_PATH=$(dirname "${SCRIPT_PATH}")
PLOT_PATH=$(dirname "${SELECTIVITY_PATH}")
ROOT_PATH=$(dirname "${PLOT_PATH}")

function append {
    x=$1
    avg_file=$2
    target_file=$3

    if [[ -f "${avg_file}" ]]; then
        content=$(cat "${avg_file}")
        if [[ -n ${content} ]]; then
            echo "${x} ${content}" >> ${target_file}
        fi
    fi
}

result_path="${ROOT_PATH}/output/result/selectivity_q1_epinions"
bash "${PLOT_PATH}/compute_average.sh" "${result_path}"

io_dat_file="${result_path}/io.dat"
rm -f ${io_dat_file}
touch ${io_dat_file}
append "0.0029" "${result_path}/task1.avg" "${io_dat_file}"
append "0.3365" "${result_path}/task5.avg" "${io_dat_file}"
append "0.6039" "${result_path}/task9.avg" "${io_dat_file}"
append "0.9123" "${result_path}/task13.avg" "${io_dat_file}"
append "1" "${result_path}/task17.avg" "${io_dat_file}"

sparkcqc_dat_file="${result_path}/sparkcqc.dat"
rm -f ${sparkcqc_dat_file}
touch ${sparkcqc_dat_file}
append "0.0029" "${result_path}/task2.avg" "${sparkcqc_dat_file}"
append "0.3365" "${result_path}/task6.avg" "${sparkcqc_dat_file}"
append "0.6039" "${result_path}/task10.avg" "${sparkcqc_dat_file}"
append "0.9123" "${result_path}/task14.avg" "${sparkcqc_dat_file}"
append "1" "${result_path}/task18.avg" "${sparkcqc_dat_file}"

sparksql_dat_file="${result_path}/sparksql.dat"
rm -f ${sparksql_dat_file}
touch ${sparksql_dat_file}
append "0.0029" "${result_path}/task3.avg" "${sparksql_dat_file}"
append "0.3365" "${result_path}/task7.avg" "${sparksql_dat_file}"
append "0.6039" "${result_path}/task11.avg" "${sparksql_dat_file}"
append "0.9123" "${result_path}/task15.avg" "${sparksql_dat_file}"
append "1" "${result_path}/task19.avg" "${sparksql_dat_file}"

postgresql_dat_file="${result_path}/postgresql.dat"
rm -f ${postgresql_dat_file}
touch ${postgresql_dat_file}
append "0.0029" "${result_path}/task4.avg" "${postgresql_dat_file}"
append "0.3365" "${result_path}/task8.avg" "${postgresql_dat_file}"
append "0.6039" "${result_path}/task12.avg" "${postgresql_dat_file}"
append "0.9123" "${result_path}/task16.avg" "${postgresql_dat_file}"
append "1" "${result_path}/task20.avg" "${postgresql_dat_file}"

cd "${ROOT_PATH}"
mkdir -p "output/figure/selectivity"
gnuplot -c "plot/selectivity/q1_epinions/plot.plt"