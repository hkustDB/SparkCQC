#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARALLEL_PATH=$(dirname "${SCRIPT_PATH}")
PLOT_PATH=$(dirname "${PARALLEL_PATH}")
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

result_path="${ROOT_PATH}/output/result/parallel_q3_epinions"
bash "${PLOT_PATH}/compute_average.sh" "${result_path}"

io_dat_file="${result_path}/io.dat"
rm -f ${io_dat_file}
touch ${io_dat_file}
append "1" "${result_path}/task1.avg" "${io_dat_file}"
append "2" "${result_path}/task6.avg" "${io_dat_file}"
append "4" "${result_path}/task11.avg" "${io_dat_file}"
append "8" "${result_path}/task16.avg" "${io_dat_file}"
append "16" "${result_path}/task21.avg" "${io_dat_file}"

sparkcqc1_dat_file="${result_path}/sparkcqc1.dat"
rm -f ${sparkcqc1_dat_file}
touch ${sparkcqc1_dat_file}
append "1" "${result_path}/task2.avg" "${sparkcqc1_dat_file}"
append "2" "${result_path}/task7.avg" "${sparkcqc1_dat_file}"
append "4" "${result_path}/task12.avg" "${sparkcqc1_dat_file}"
append "8" "${result_path}/task17.avg" "${sparkcqc1_dat_file}"
append "16" "${result_path}/task22.avg" "${sparkcqc1_dat_file}"

sparkcqc2_dat_file="${result_path}/sparkcqc2.dat"
rm -f ${sparkcqc2_dat_file}
touch ${sparkcqc2_dat_file}
append "1" "${result_path}/task3.avg" "${sparkcqc2_dat_file}"
append "2" "${result_path}/task8.avg" "${sparkcqc2_dat_file}"
append "4" "${result_path}/task13.avg" "${sparkcqc2_dat_file}"
append "8" "${result_path}/task18.avg" "${sparkcqc2_dat_file}"
append "16" "${result_path}/task23.avg" "${sparkcqc2_dat_file}"

sparksql_dat_file="${result_path}/sparksql.dat"
rm -f ${sparksql_dat_file}
touch ${sparksql_dat_file}
append "1" "${result_path}/task4.avg" "${sparksql_dat_file}"
append "2" "${result_path}/task9.avg" "${sparksql_dat_file}"
append "4" "${result_path}/task14.avg" "${sparksql_dat_file}"
append "8" "${result_path}/task19.avg" "${sparksql_dat_file}"
append "16" "${result_path}/task24.avg" "${sparksql_dat_file}"

postgresql_dat_file="${result_path}/postgresql.dat"
rm -f ${postgresql_dat_file}
touch ${postgresql_dat_file}
append "1" "${result_path}/task5.avg" "${postgresql_dat_file}"
append "2" "${result_path}/task10.avg" "${postgresql_dat_file}"
append "4" "${result_path}/task15.avg" "${postgresql_dat_file}"
append "8" "${result_path}/task20.avg" "${postgresql_dat_file}"
append "16" "${result_path}/task25.avg" "${postgresql_dat_file}"

cd "${ROOT_PATH}"
mkdir -p "output/figure/parallel_processing"
gnuplot -c "plot/parallel_processing/q3_epinions/plot.plt"