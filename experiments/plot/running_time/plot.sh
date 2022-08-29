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
    target_file=$5

    io_avg=$(load "${io_avg_file}")
    sparkcqc_avg=$(load "${sparkcqc_avg_file}")
    sparksql_avg=$(load "${sparksql_avg_file}")
    postgresql_avg=$(load "${postgresql_avg_file}")

    echo "${io_avg} ${sparkcqc_avg} ${sparksql_avg} ${postgresql_avg}" >> "${target_file}"
}

target_file_path="${ROOT_PATH}/output/result/running_time"
target_file="${target_file_path}/result.dat"
mkdir -p "${target_file_path}"
rm -f "${target_file}"
touch "${target_file}"

# Q1
result_path_q1="${ROOT_PATH}/output/result/q1"
bash "${PLOT_PATH}/compute_average.sh" "${result_path_q1}"

# Q1-Epinions
append "${result_path_q1}/task1.avg" "${result_path_q1}/task2.avg" "${result_path_q1}/task3.avg" "${result_path_q1}/task4.avg" "${target_file}"

# Q1-Google
append "${result_path_q1}/task5.avg" "${result_path_q1}/task6.avg" "${result_path_q1}/task7.avg" "${result_path_q1}/task8.avg" "${target_file}"

# Q1-Wiki
append "${result_path_q1}/task9.avg" "${result_path_q1}/task10.avg" "${result_path_q1}/task11.avg" "${result_path_q1}/task12.avg" "${target_file}"

# Q2
result_path_q2="${ROOT_PATH}/output/result/q2"
bash "${PLOT_PATH}/compute_average.sh" "${result_path_q2}"

# Q2-Bitcoin
append "${result_path_q2}/task1.avg" "${result_path_q2}/task2.avg" "${result_path_q2}/task3.avg" "${result_path_q2}/task4.avg" "${target_file}"

# Q3
result_path_q3="${ROOT_PATH}/output/result/q3"
bash "${PLOT_PATH}/compute_average.sh" "${result_path_q3}"

# Q3-Epinions
append "${result_path_q3}/task1.avg" "${result_path_q3}/task2.avg" "${result_path_q3}/task3.avg" "${result_path_q3}/task4.avg" "${target_file}"

# Q3-Google
append "${result_path_q3}/task5.avg" "${result_path_q3}/task6.avg" "${result_path_q3}/task7.avg" "${result_path_q3}/task8.avg" "${target_file}"

# Q3-Wiki
append "${result_path_q3}/task9.avg" "${result_path_q3}/task10.avg" "${result_path_q3}/task11.avg" "${result_path_q3}/task12.avg" "${target_file}"

# Q4
result_path_q4="${ROOT_PATH}/output/result/q4"
bash "${PLOT_PATH}/compute_average.sh" "${result_path_q4}"

# Q4-Epinions
append "${result_path_q4}/task1.avg" "${result_path_q4}/task2.avg" "${result_path_q4}/task3.avg" "${result_path_q4}/task4.avg" "${target_file}"

# Q4-Google
append "${result_path_q4}/task5.avg" "${result_path_q4}/task6.avg" "${result_path_q4}/task7.avg" "${result_path_q4}/task8.avg" "${target_file}"

# Q4-Wiki
append "${result_path_q4}/task9.avg" "${result_path_q4}/task10.avg" "${result_path_q4}/task11.avg" "${result_path_q4}/task12.avg" "${target_file}"

# Q5
result_path_q5="${ROOT_PATH}/output/result/q5"
bash "${PLOT_PATH}/compute_average.sh" "${result_path_q5}"

# Q5-DBLP
append "${result_path_q5}/task1.avg" "${result_path_q5}/task2.avg" "${result_path_q5}/task3.avg" "${result_path_q5}/task4.avg" "${target_file}"

# Q5-Google
append "${result_path_q5}/task5.avg" "${result_path_q5}/task6.avg" "${result_path_q5}/task7.avg" "${result_path_q5}/task8.avg" "${target_file}"

# Q6
result_path_q6="${ROOT_PATH}/output/result/q6"
bash "${PLOT_PATH}/compute_average.sh" "${result_path_q6}"
append "${result_path_q6}/task1.avg" "${result_path_q6}/task2.avg" "${result_path_q6}/task3.avg" "${result_path_q6}/task4.avg" "${target_file}"

# Q7
result_path_q7="${ROOT_PATH}/output/result/q7"
bash "${PLOT_PATH}/compute_average.sh" "${result_path_q7}"
append "${result_path_q7}/task1.avg" "${result_path_q7}/task2.avg" "${result_path_q7}/task3.avg" "${result_path_q7}/task4.avg" "${target_file}"

# Q8
result_path_q8="${ROOT_PATH}/output/result/q8"
bash "${PLOT_PATH}/compute_average.sh" "${result_path_q8}"
append "${result_path_q8}/task1.avg" "${result_path_q8}/task2.avg" "${result_path_q8}/task3.avg" "${result_path_q8}/task4.avg" "${target_file}"

cd "${ROOT_PATH}"
mkdir -p "output/figure/running_time"
gnuplot -c "plot/running_time/plot.plt"