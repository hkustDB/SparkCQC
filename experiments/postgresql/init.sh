#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

source "${PARENT_PATH}/common.sh"

config_files=("${PARENT_PATH}/config.properties")

log_file="${PARENT_PATH}/log/pg_init.log"

mkdir -p "${PARENT_PATH}/log"
rm -f ${log_file}
touch ${log_file}

postgresql_home=$(prop ${config_files} 'postgresql.home')
psql="${postgresql_home}/bin/psql"
database=$(prop ${config_files} 'postgresql.database')
username=$(prop ${config_files} 'postgresql.username')
port=$(prop ${config_files} 'postgresql.port')

create_tables_sql="${SCRIPT_PATH}/create_tables.sql"
${psql} "-d" "${database}" "-U" "${username}" "-p" "${port}" "-f" "${create_tables_sql}" >> ${log_file} 2>&1

load_data_sql="${SCRIPT_PATH}/load_data.sql"
rm -f "${load_data_sql}"
touch "${load_data_sql}"

echo "DELETE FROM bitcoin;" >> ${load_data_sql}
echo "DELETE FROM epinions;" >> ${load_data_sql}
echo "DELETE FROM google;" >> ${load_data_sql}
echo "DELETE FROM wiki;" >> ${load_data_sql}
echo "DELETE FROM dblp;" >> ${load_data_sql}
echo "DELETE FROM trade;" >> ${load_data_sql}
echo "DELETE FROM holding;" >> ${load_data_sql}

echo "COPY bitcoin FROM '${PARENT_PATH}/data/bitcoin.txt' DELIMITER ',' CSV;" >> ${load_data_sql}
echo "COPY epinions FROM '${PARENT_PATH}/data/epinions.txt' DELIMITER ' ' CSV;" >> ${load_data_sql}
echo "COPY google FROM '${PARENT_PATH}/data/google.txt' DELIMITER ' ' CSV;" >> ${load_data_sql}
echo "COPY wiki FROM '${PARENT_PATH}/data/wiki.txt' DELIMITER ' ' CSV;" >> ${load_data_sql}
echo "COPY dblp FROM '${PARENT_PATH}/data/dblp.txt' DELIMITER ' ' CSV;" >> ${load_data_sql}
echo "COPY trade FROM '${PARENT_PATH}/data/trade.txt' DELIMITER '|' CSV;" >> ${load_data_sql}
echo "COPY holding FROM '${PARENT_PATH}/data/holding.txt' DELIMITER '|' CSV;" >> ${load_data_sql}

${psql} "-d" "${database}" "-U" "${username}" "-p" "${port}" "-f" "${load_data_sql}" >> ${log_file} 2>&1