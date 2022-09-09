#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

source "${SCRIPT_PATH}/common.sh"

config_files=("${SCRIPT_PATH}/config.properties")

args_spec=''
args_task=''
args_all='false'
execution_time=''

function usage {
    echo "Usage: execute.sh [Options]

Options:
  -h, --help                Print this message.
  -a, --all                 Run all the spec files.
  -s, --spec <spec file>    Run the specific spec file. (will be ignored if '-a' is set)
  -t, --task <task ID>      Run the specific task only. (will be ignored if '-a' is set)

Examples:
  (1) execute -a
	Run all the spec files.

  (2) execute -s specs/parallel_processing/q2_bitcoin/q2_bitcoin.spec
	Run the q2_bitcoin.spec in parallel experiment.

  (3) execute -s specs/parallel_processing/q2_bitcoin/q2_bitcoin.spec -t 10
	Run only the task10 in q2_bitcoin.spec in parallel experiment.
"
    exit 1
}

opt_args=$(getopt -a -n execute -o hs:t:a --long help,spec:,task:all -- "$@")
if [[ $? -ne 0 ]]; then
    usage
fi

eval set -- "${opt_args}"
while :; do
    case $1 in
        -h | --help) usage ;;
        -s | --spec) args_spec=$2; shift 2 ;;
        -t | --task) args_task=$2; shift 2 ;;
        -a | --all) args_all='true'; shift 1 ;;
        --) shift; break ;;
        *) echo "Unexpected option: $1."; usage ;;
    esac
done

function execute_spec {
    spec_file=$1

    experiment_name=$(prop "${spec_file}" 'spec.experiment.name' 'default')
    log_file="${SCRIPT_PATH}/log/execute_${experiment_name}.log"

    mkdir -p "${SCRIPT_PATH}/log"
    rm -f ${log_file}
    touch ${log_file}

    echo "executing spec: ${spec_file}" >> ${log_file}

    spec_result_path="${SCRIPT_PATH}/output/result/${experiment_name}"
    mkdir -p "${spec_result_path}"

    repeat_count=$(prop ${config_files} "common.experiment.repeat")
    current_repeat=1
    while [[ ${current_repeat} -le ${repeat_count} ]]
    do
        if [[ -z ${args_task} ]]; then
            task_count=$(prop "${spec_file}" "spec.tasks.count")
            current_task=1
            while [[ ${current_task} -le ${task_count} ]]
            do
                execute_task ${spec_file} ${current_task} ${spec_result_path}
                current_task=$(($current_task+1))
            done
        else
            execute_task ${spec_file} ${args_task} ${spec_result_path}
        fi

        current_repeat=$((current_repeat+1))
    done
}

function execute_task {
    spec_file=$1
    current_task=$2
    spec_result_path=$3
    task_result_file="${spec_result_path}/task${current_task}.txt"
    echo "executing task: ${current_task}" >> ${log_file}

    execution_time='-1'
    touch ${task_result_file}

    system_name=$(prop ${spec_file} "task${current_task}.system.name")
    if [[ "${system_name}" == "io" ]]; then
        class_name=$(prop2 ${spec_file} "task${current_task}.io.classname" "task.io.classname")
        cores_max=$(prop2 ${spec_file} "task${current_task}.io.cores.max" "task.io.cores.max")
        default_parallelism=$(prop2 ${spec_file} "task${current_task}.io.default.parallelism" "task.io.default.parallelism")
        driver_memory=$(prop2 ${spec_file} "task${current_task}.io.driver.memory" "task.io.driver.memory")
        executor_cores=$(prop2 ${spec_file} "task${current_task}.io.executor.cores" "task.io.executor.cores")
        executor_memory=$(prop2 ${spec_file} "task${current_task}.io.executor.memory" "task.io.executor.memory")
        main_args1="${SCRIPT_PATH}/data"
        main_args2=$(prop2 ${spec_file} "task${current_task}.io.graph.name" "task.io.graph.name")
        tmp_path=$(prop ${config_files} "common.tmp.path")
        k_value=$(prop2 ${spec_file} "task${current_task}.io.k.value" "task.io.k.value")
        execute_io ${class_name} ${cores_max} ${default_parallelism} ${driver_memory} \
        ${executor_cores} ${executor_memory} ${main_args1} ${main_args2} ${k_value} "${tmp_path}/tmp_io"
    elif [[ "${system_name}" == "io_huge" ]]; then
        class_name=$(prop2 ${spec_file} "task${current_task}.io_huge.classname" "task.io_huge.classname")
        cores_max=$(prop2 ${spec_file} "task${current_task}.io_huge.cores.max" "task.io_huge.cores.max")
        default_parallelism=$(prop2 ${spec_file} "task${current_task}.io_huge.default.parallelism" "task.io_huge.default.parallelism")
        driver_memory=$(prop2 ${spec_file} "task${current_task}.io_huge.driver.memory" "task.io_huge.driver.memory")
        executor_cores=$(prop2 ${spec_file} "task${current_task}.io_huge.executor.cores" "task.io_huge.executor.cores")
        executor_memory=$(prop2 ${spec_file} "task${current_task}.io_huge.executor.memory" "task.io_huge.executor.memory")
        main_args1="${SCRIPT_PATH}/data"
        main_args2=$(prop2 ${spec_file} "task${current_task}.io_huge.graph.name" "task.io_huge.graph.name")
        tmp_path=$(prop ${config_files} "common.tmp.path")
        k_value=$(prop2 ${spec_file} "task${current_task}.io_huge.k.value" "task.io_huge.k.value")
        execute_io_huge ${class_name} ${cores_max} ${default_parallelism} ${driver_memory} \
        ${executor_cores} ${executor_memory} ${main_args1} ${main_args2} ${k_value} "${tmp_path}/tmp_io"
    elif [[ "${system_name}" == "cqc" ]]; then
        class_name=$(prop2 ${spec_file} "task${current_task}.cqc.classname" "task.cqc.classname")
        cores_max=$(prop2 ${spec_file} "task${current_task}.cqc.cores.max" "task.cqc.cores.max")
        default_parallelism=$(prop2 ${spec_file} "task${current_task}.cqc.default.parallelism" "task.cqc.default.parallelism")
        driver_memory=$(prop2 ${spec_file} "task${current_task}.cqc.driver.memory" "task.cqc.driver.memory")
        executor_cores=$(prop2 ${spec_file} "task${current_task}.cqc.executor.cores" "task.cqc.executor.cores")
        executor_memory=$(prop2 ${spec_file} "task${current_task}.cqc.executor.memory" "task.cqc.executor.memory")
        main_args1="${SCRIPT_PATH}/data"
        main_args2=$(prop2 ${spec_file} "task${current_task}.cqc.graph.name" "task.cqc.graph.name")
        k_value=$(prop2 ${spec_file} "task${current_task}.cqc.k.value" "task.cqc.k.value")
        execute_spark ${class_name} ${cores_max} ${default_parallelism} ${driver_memory} \
        ${executor_cores} ${executor_memory} ${main_args1} ${main_args2} ${k_value} "no_io" "dummy"
    elif [[ "${system_name}" == "spark" ]]; then
        class_name=$(prop2 ${spec_file} "task${current_task}.spark.classname" "task.spark.classname")
        cores_max=$(prop2 ${spec_file} "task${current_task}.spark.cores.max" "task.spark.cores.max")
        default_parallelism=$(prop2 ${spec_file} "task${current_task}.spark.default.parallelism" "task.spark.default.parallelism")
        driver_memory=$(prop2 ${spec_file} "task${current_task}.spark.driver.memory" "task.spark.driver.memory")
        executor_cores=$(prop2 ${spec_file} "task${current_task}.spark.executor.cores" "task.spark.executor.cores")
        executor_memory=$(prop2 ${spec_file} "task${current_task}.spark.executor.memory" "task.spark.executor.memory")
        main_args1="${SCRIPT_PATH}/data"
        main_args2=$(prop2 ${spec_file} "task${current_task}.spark.graph.name" "task.spark.graph.name")
        k_value=$(prop2 ${spec_file} "task${current_task}.spark.k.value" "task.spark.k.value")
        execute_spark ${class_name} ${cores_max} ${default_parallelism} ${driver_memory} \
        ${executor_cores} ${executor_memory} ${main_args1} ${main_args2} ${k_value}
    elif [[ "${system_name}" == "postgresql" ]]; then
        query=$(prop2 ${spec_file} "task${current_task}.postgresql.query" "task.postgresql.query")
        parallelism=$(prop2 ${spec_file} "task${current_task}.postgresql.parallelism" "task.postgresql.parallelism")
        execute_postgresql ${query} ${parallelism}
    elif [[ "${system_name}" == "any_k" ]]; then
        class_name=$(prop2 ${spec_file} "task${current_task}.any_k.classname" "task.any_k.classname")
        in_file=$(prop2 ${spec_file} "task${current_task}.any_k.in.file" "task.any_k.in.file")
        memory=$(prop2 ${spec_file} "task${current_task}.any_k.memory" "task.any_k.memory")
        execute_any_k ${class_name} ${in_file} ${memory}
    else
        err "system name must be io, io_huge, cqc, spark, any_k, or postgresql"
    fi

    echo ${execution_time} >> ${task_result_file}
}

function execute_io() {
    timeout_time=$(prop ${config_files} 'common.experiment.timeout')
    cqc_home="${PARENT_PATH}"
    cqc_jar="${cqc_home}/target/ComparisonJoins-1.0-SNAPSHOT.jar"
    class_name=$1
    cores_max=$2
    default_parallelism=$3
    driver_memory=$4
    executor_cores=$5
    executor_memory=$6

    spark_home=$(prop ${config_files} 'spark.home')
    spark_submit="${spark_home}/bin/spark-submit"
    spark_master="local[${cores_max}]"

    data_path=$7
    graph_name=$8
    k_value=$9
    io_path=${10}

    cd ${SCRIPT_PATH}

    rm -rf "${io_path}"

    timeout -s SIGKILL "${timeout_time}" ${spark_submit} "--class" "${class_name}" "--master" "${spark_master}" \
    "--conf" "spark.cores.max=${cores_max}" "--conf" "spark.default.parallelism=${default_parallelism}" \
    "--conf" "spark.driver.memory=${driver_memory}" "--conf" "spark.executor.cores=${executor_cores}" \
    "--conf" "spark.executor.memory=${executor_memory}" \
    "${cqc_jar}" "${data_path}" "${graph_name}" "${k_value}" "normal_io" "${io_path}" >> ${log_file} 2>&1

    status_code=$?
    if [[ ${status_code} -eq 137 ]]; then
        return 1
    elif [[ ${status_code} -ne 0 ]]; then
        return 1
    fi

    class_name="org.SparkCQC.GraphLoading"
    timeout -s SIGKILL "${timeout_time}" ${spark_submit} "--class" "${class_name}" "--master" "${spark_master}" \
    "--conf" "spark.cores.max=${cores_max}" "--conf" "spark.default.parallelism=${default_parallelism}" \
    "--conf" "spark.driver.memory=${driver_memory}" "--conf" "spark.executor.cores=${executor_cores}" \
    "--conf" "spark.executor.memory=${executor_memory}" \
    "${cqc_jar}" "${io_path}" >> ${log_file} 2>&1

    status_code=$?
    if [[ ${status_code} -eq 137 ]]; then
        return 1
    elif [[ ${status_code} -ne 0 ]]; then
        return 1
    else
        extracted_time=$(tail -n 20 ${log_file} | grep "Time taken: " | tail -n 1 | sed -rn 's/^.*Time taken:\s*(\S+).*$/\1/p')
        if [[ -n ${extracted_time} ]]; then
            execution_time=${extracted_time}
        fi
    fi
}

function execute_io_huge() {
    timeout_time=$(prop ${config_files} 'common.experiment.timeout')
    cqc_home="${PARENT_PATH}"
    cqc_jar="${cqc_home}/target/ComparisonJoins-1.0-SNAPSHOT.jar"
    class_name=$1
    cores_max=$2
    default_parallelism=$3
    driver_memory=$4
    executor_cores=$5
    executor_memory=$6

    spark_home=$(prop ${config_files} 'spark.home')
    spark_submit="${spark_home}/bin/spark-submit"
    spark_master="local[${cores_max}]"

    data_path=$7
    graph_name=$8
    k_value=$9
    io_path=${10}

    cd ${SCRIPT_PATH}

    rm -rf "${io_path}"

    timeout -s SIGKILL "${timeout_time}" ${spark_submit} "--class" "${class_name}" "--master" "${spark_master}" \
    "--conf" "spark.cores.max=${cores_max}" "--conf" "spark.default.parallelism=${default_parallelism}" \
    "--conf" "spark.driver.memory=${executor_memory}" "--conf" "spark.executor.cores=${executor_cores}" \
    "--conf" "spark.executor.memory=${executor_memory}" \
    "${cqc_jar}" "${data_path}" "${graph_name}" "${k_value}" "huge_io" "${io_path}" >> ${log_file} 2>&1

    status_code=$?
    if [[ ${status_code} -eq 137 ]]; then
        return 1
    elif [[ ${status_code} -ne 0 ]]; then
        return 1
    fi

    class_name="org.SparkCQC.GraphLoading"
    # randomly pick 4 part-* files for loading
    # all the io_huge tasks should have default.parallelism = 4n
    # since the loading maybe very time consuming, we just measure 4 part-* files and report n times
    # the sum of the cost.
    timeout -s SIGKILL "${timeout_time}" ${spark_submit} "--class" "${class_name}" "--master" "${spark_master}" \
    "--conf" "spark.cores.max=${cores_max}" "--conf" "spark.default.parallelism=${default_parallelism}" \
    "--conf" "spark.driver.memory=${executor_memory}" "--conf" "spark.executor.cores=${executor_cores}" \
    "--conf" "spark.executor.memory=${executor_memory}" \
    "${cqc_jar}" "${io_path}" >> ${log_file} 2>&1

    status_code=$?
    if [[ ${status_code} -eq 137 ]]; then
        return 1
    elif [[ ${status_code} -ne 0 ]]; then
        return 1
    else
        extracted_time=$(tail -n 20 ${log_file} | grep "Time taken: " | tail -n 1 | sed -rn 's/^.*Time taken:\s*(\S+).*$/\1/p')
        if [[ -n ${extracted_time} ]]; then
            current_execution_time=${extracted_time}
            echo "execution time for file: ${io_path} is ${current_execution_time} ms" >> ${log_file}
            # write the execution_time_sum * parallelism into execution_time
            echo "default_parallelism = ${default_parallelism}"
            # e.g., assume we have load the tmp file in 90000 ms, and the parallelism is 32,
            # since the tmp file contains only 1/32 rows of the whole result
            # the total execution_time should be 90000 * 32 = 2880000 ms
            execution_time=$(echo "${current_execution_time} * ${default_parallelism}" | bc)
        else
            return 1
        fi
    fi
}

function execute_spark {
    timeout_time=$(prop ${config_files} 'common.experiment.timeout')
    spark_home=$(prop ${config_files} 'spark.home')
    spark_submit="${spark_home}/bin/spark-submit"
    spark_master=$(prop ${config_files} 'spark.master.url')
    cqc_home="${PARENT_PATH}"
    cqc_jar="${cqc_home}/target/ComparisonJoins-1.0-SNAPSHOT.jar"
    class_name=$1
    cores_max=$2
    default_parallelism=$3
    driver_memory=$4
    executor_cores=$5
    executor_memory=$6
    main_args1=$7
    main_args2=$8
    main_args3=$9
    main_args4=${10}
    main_args5=${11}

    cd ${SCRIPT_PATH}

    timeout -s SIGKILL "${timeout_time}" ${spark_submit} "--class" "${class_name}" "--master" "${spark_master}" \
    "--conf" "spark.cores.max=${cores_max}" "--conf" "spark.default.parallelism=${default_parallelism}" \
    "--conf" "spark.driver.memory=${driver_memory}" "--conf" "spark.executor.cores=${executor_cores}" \
    "--conf" "spark.executor.memory=${executor_memory}" \
    "${cqc_jar}" "${main_args1}" "${main_args2}" "${main_args3}" "${main_args4}" "${main_args5}" >> ${log_file} 2>&1

    status_code=$?
    if [[ ${status_code} -eq 137 ]]; then
        return 1
    elif [[ ${status_code} -ne 0 ]]; then
        return 1
    else
        extracted_time=$(tail -n 20 ${log_file} | grep "Time taken: " | tail -n 1 | sed -rn 's/^.*Time taken:\s*(\S+).*$/\1/p')
        if [[ -n ${extracted_time} ]]; then
            execution_time=${extracted_time}
        fi
    fi
}

function execute_postgresql {
    timeout_time=$(prop ${config_files} 'common.experiment.postgresql.timeout')
    postgresql_home=$(prop ${config_files} 'postgresql.home')
    psql="${postgresql_home}/bin/psql"
    database=$(prop ${config_files} 'postgresql.database')
    username=$(prop ${config_files} 'postgresql.username')
    port=$(prop ${config_files} 'postgresql.port')
    input_query=$1
    parallelism=$2

    cd ${SCRIPT_PATH}

    # create query file under tmp path
    tmp_path=$(prop ${config_files} "common.tmp.path")
    submit_query="${tmp_path}/postgres_query.sql"

    rm -f "${submit_query}"
    touch "${submit_query}"

    echo "SET max_parallel_workers_per_gather=$2;" >> ${submit_query}
    echo "SET max_parallel_workers=$2;" >> ${submit_query}
    echo "SET statement_timeout=${timeout_time};" >> ${submit_query}
    echo "COPY (" >> ${submit_query}
    cat ${input_query} >> ${submit_query}
    echo ") TO '/dev/null' DELIMITER ',' CSV;" >> ${submit_query}

    ${psql} "-d" "${database}" "-U" "${username}" "-p" "${port}" \
    "-c" '\timing' "-f" "${submit_query}" >> ${log_file} 2>&1

    status_code=$?
    if [[ ${status_code} -eq 137 ]]; then
        return 1
    elif [[ ${status_code} -ne 0 ]]; then
        return 1
    else
        extracted_time=$(tail -n 20 ${log_file} | grep -A20 "COPY" | grep "Time: " | tail -n 1 | sed -rn 's/^.*Time:\s*(\S+).*$/\1/p')
        if [[ -n ${extracted_time} ]]; then
            execution_time=${extracted_time}
        fi
    fi
}

function execute_any_k {
    timeout_time=$(prop ${config_files} 'common.experiment.timeout')
    class_name=$1
    in_file=$2
    memory=$3

    cd ${SCRIPT_PATH}

    timeout -s SIGKILL "${timeout_time}" java "-Xms${memory}" "-Xmx${memory}" "-cp" "${SCRIPT_PATH}/any_k/anyk-code/target/any-k-1.0.jar" "${class_name}" "${SCRIPT_PATH}/data/${in_file}" >> ${log_file} 2>&1

    status_code=$?
    if [[ ${status_code} -eq 137 ]]; then
        return 1
    elif [[ ${status_code} -ne 0 ]]; then
        return 1
    else
        extracted_time=$(tail -n 20 ${log_file} | grep -A20 "Dummy counter" | grep "Time: " | tail -n 1 | sed -rn 's/^.*Time:\s*(\S+).*$/\1/p')
        if [[ -n ${extracted_time} ]]; then
            execution_time=${extracted_time}
        fi
    fi
}

execute_log_file="${SCRIPT_PATH}/log/execute.log"
mkdir -p "${SCRIPT_PATH}/log"
touch "${SCRIPT_PATH}/log/execute.log"

if [[ ${args_all} == 'true' ]]; then
    # ignore -t argument
    args_task=''
    # execute all specs
    execute_spec "${SCRIPT_PATH}/specs/graph_queries/q1/q1.spec"
    execute_spec "${SCRIPT_PATH}/specs/graph_queries/q2/q2.spec"
    execute_spec "${SCRIPT_PATH}/specs/graph_queries/q3/q3.spec"
    execute_spec "${SCRIPT_PATH}/specs/graph_queries/q4/q4.spec"
    execute_spec "${SCRIPT_PATH}/specs/graph_queries/q5/q5.spec"
    echo "finish executing graph queries" >> "${execute_log_file}"

    execute_spec "${SCRIPT_PATH}/specs/analytical_queries/q6/q6.spec"
    execute_spec "${SCRIPT_PATH}/specs/analytical_queries/q7/q7.spec"
    execute_spec "${SCRIPT_PATH}/specs/analytical_queries/q8/q8.spec"
    echo "finish executing analytical queries" >> "${execute_log_file}"

    execute_spec "${SCRIPT_PATH}/specs/selectivity/q1_epinions/q1_epinions.spec"
    execute_spec "${SCRIPT_PATH}/specs/selectivity/q2_bitcoin/q2_bitcoin.spec"
    execute_spec "${SCRIPT_PATH}/specs/selectivity/q3_epinions/q3_epinions.spec"
    echo "finish executing selectivity queries" >> "${execute_log_file}"

    execute_spec "${SCRIPT_PATH}/specs/parallel_processing/q1_epinions/q1_epinions.spec"
    execute_spec "${SCRIPT_PATH}/specs/parallel_processing/q2_bitcoin/q2_bitcoin.spec"
    execute_spec "${SCRIPT_PATH}/specs/parallel_processing/q3_epinions/q3_epinions.spec"
    echo "finish executing parallel processing" >> "${execute_log_file}"

    execute_spec "${SCRIPT_PATH}/specs/different_systems/different_systems.spec"
    echo "finish executing different systems" >> "${execute_log_file}"

    echo "finish executing all specs" >> "${execute_log_file}"
else
    if [[ -z ${args_spec} ]]; then
        echo "spec file must be provided if '-a' is unset."
        usage
    fi

    execute_spec ${args_spec}
    echo "finish executing spec: ${args_spec}" >> "${execute_log_file}"
fi