#!/bin/bash

# This script will check the following:
# 1. SparkCQC has been compiled
# 2. the graph data files exist
# 3. the tpc-e tool files exist
# 4. the tpc-e data files exist
# 5. the spark.home is set properly
# 6. the prepared versions of the graphs exist
# 7. PostgreSQL server is running and command psql is able to connect with the server
# 8. the tables in PostgreSQL is all created and non-empty
# 9. Any-K has been compiled


SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

source "${SCRIPT_PATH}/common.sh"

config_files=("${SCRIPT_PATH}/config.properties")

log_file="${SCRIPT_PATH}/log/build.log"

mkdir -p "${SCRIPT_PATH}/log"
rm -f ${log_file}
touch ${log_file}

echo "Checking SparkCQC"
spark_cqc_home="${PARENT_PATH}"
if [[ ! -f "${spark_cqc_home}/target/ComparisonJoins-1.0-SNAPSHOT.jar" ]]; then
    err "SparkCQC needs to be built first."
    err "Please try running 'bash build.sh' to build SparkCQC."
    exit 1
fi

echo "Checking graph data"
data_home="${SCRIPT_PATH}/data"
graph_names="bitcoin epinions google wiki dblp"
for graph_name in ${graph_names[@]}; do
    if [[ ! -f "${data_home}/${graph_name}.txt" ]]; then
        err "Missing data file for graph ${graph_name}"
        err "Please try running 'bash data/create_graph_data.sh' to create graph files."
        exit 1
    fi
done

echo "Checking tpc-e tool"
tpc_e_home="${SCRIPT_PATH}/tpc_e"
tpc_e_tool_home="${tpc_e_home}/tpc_e_tool"
tpc_e_tool_cmd="${tpc_e_tool_home}/bin/EGenLoader"
if [[ ! -f "${tpc_e_tool_cmd}" ]]; then
    err "Missing tpc-e-tool file."
    err "Please download tpc-e-tool.zip from https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp and move it into tpc_e/"
    err "Then try running 'bash tpc_e/build_tpc_e_tool.sh' to build tpc-e-tool."
    exit 1
fi

echo "Checking tpc-e data"
data_home="${SCRIPT_PATH}/data"
data_files="trade.txt tradeB.txt tradeS.txt trade.in"
for data_file in ${data_files[@]}; do
    if [[ ! -f "${data_home}/${data_file}" ]]; then
        err "Missing tpc-e data file ${data_file}"
        err "Please try running 'bash tpc_e/generate_tpc_e_data.sh' to generate tpc-e data files."
        err "Then try running 'bash data/create_tpc_e_data.sh' to convert the data files to a suitable format."
        exit 1
    fi
done

echo "Checking Spark"
spark_home=$(prop ${config_files} 'spark.home')
spark_submit="${spark_home}/bin/spark-submit"
if [[ ! -f "${spark_submit}" ]]; then
    err "Property 'spark.home' is not set properly. spark-submit is not found under ${spark_home}/bin/"
    err "Please set 'spark.home' in config.properties properly."
    exit 1
fi

echo "Checking graphs preparation"
data_files="bitcoin-txt-prepared epinions-txt-prepared google-txt-prepared wiki-txt-prepared dblp-txt-prepared"
for data_file in ${data_files[@]}; do
    if [[ ! -d "${data_home}/${data_file}" ]]; then
        err "Missing prepared data file ${data_file}."
        err "Please try running 'bash data/prepare_graph_data.sh' to create the files."
        exit 1
    fi
done

echo "Checking PostgreSQL server"
postgresql_home=$(prop ${config_files} 'postgresql.home')
psql="${postgresql_home}/bin/psql"
database=$(prop ${config_files} 'postgresql.database')
username=$(prop ${config_files} 'postgresql.username')
port=$(prop ${config_files} 'postgresql.port')
if [[ ! -f "${psql}" ]]; then
    err "Property 'postgresql.home' is not set properly. psql is not found under ${postgresql_home}/bin/"
    err "Please set 'postgresql.home' in config.properties properly."
    exit 1
fi

${psql} "-d" "${database}" "-U" "${username}" "-p" "${port}" "-c" "SELECT 1" > /dev/null 2>&1
if [[ $? -ne 0 ]]; then
    err "psql is unable to connect with server with database: ${database}, user: ${username}, port: ${port}"
    err "Please set 'postgresql.database', 'postgresql.username', 'postgresql.port' in config.properties properly."
    err "Please also check if the postgresql server is running."
    exit 1
fi

echo "Checking tables in PostgreSQL"
table_names="bitcoin epinions google wiki dblp trade"
for table in ${table_names[@]}; do
    psql_result=$("${psql}" "-d" "${database}" "-U" "${username}" "-p" "${port}" "-c" "SELECT COUNT(*) FROM ${table}")
    if [[ $? -ne 0 ]]; then
        err "Unable to invoke the COUNT(*) on table ${table}. Please check if the table is created."
        err "Please try running 'bash postgresql/init.sh' to create the tables."
        exit 1
    else
        rows=$(echo ${psql_result} | awk -v FS=" " '{print $3}')
        if [[ ${rows} -eq 0 ]]; then
            err "Table ${table} is empty."
            err "Please try running 'bash postgresql/init.sh' to load data into the tables."
            exit 1
        fi
    fi
done

echo "Checking Any-K"
any_k_home="${SCRIPT_PATH}/any_k"
if [[ ! -f "${any_k_home}/anyk-code/target/any-k-1.0.jar" ]]; then
    err "Any-K should be compiled."
    exit 1
fi

echo ""
echo "All checks passed."