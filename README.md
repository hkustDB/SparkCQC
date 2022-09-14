# SparkCQC
This project provides the source code of our paper:
[Conjunctive Queries with Comparisons](https://dl.acm.org/doi/10.1145/3514221.3517830). It also contains the scripts for reproducing the experiment results in the paper.

## Queries

This project provides a simple demo of SparkCQC over the following 8 SQL queries:

### Query 1
```
select g1.src, g1.dst, g2.dst, g3.dst, c1.cnt, c2.cnt
from Graph g1, Graph g2, Graph g3,
(select src, count(*) as cnt from Graph group by src) as c1,
(select src, count(*) as cnt from Graph group by src) as c2
where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
and g3.dst = c2.src and c1.cnt < c2.cnt
```

### Query 2
```
SELECT g1.src, g2.src, g3.src, g4.src, g5.src, g6.src
From Graph g1, Graph g2, Graph g3, Graph g4, Graph g5, Graph g6, Graph g7
where g1.src = g3.dst and g2.src = g1.dst and g3.src=g2.dst
and g4.src = g6.dst and g5.src = g4.dst and g6.src = g5.dst
and g1.dst = g7.src and g4.src = g7.dst and
g1.weight * g2.weight * g3.weight < g4.weight * g5.weight * g6.weight
```

### Query 3
```
select g1.src, g1.dst, g2.dst, g3.dst, c1.cnt, c2.cnt, c3.cnt, c4.cnt
from Graph g1, Graph g2, Graph g3,
(select src, count(*) as cnt from Graph group by src) as c1,
(select src, count(*) as cnt from Graph group by src) as c2,
(select src, count(*) as cnt from Graph group by src) as c3,
(select dst, count(*) as cnt from Graph group by dst) as c4
where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
and g3.dst = c2.src and g3.dst = c4.dst and g2.src = c3.src
and c1.cnt < c2.cnt and c3.cnt < c4.cnt
```

### Query 4
```
select DISTINCT(g3.src, g3.dst)
from Graph g1, Graph g2, Graph g3,
(select src, count(*) as cnt from Graph group by src) as c1,
(select src, count(*) as cnt from Graph group by src) as c2
where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
and g3.dst = c2.src and c1.cnt < c2.cnt
```

### Query 5
```
select DISTINCT(g2.src, g2.dst)
from Graph g1, Graph g2, Graph g3, Graph g4, Graph g5,
(select src, count(*) as cnt from Graph group by src) as c1,
(select src, count(*) as cnt from Graph group by src) as c2,
(select dst, count(*) as cnt from Graph group by dst) as c3,
(select dst, count(*) as cnt from Graph group by dst) as c4
where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
and g3.dst = c2.src and c1.cnt < c2.cnt
and g4.dst = g2.src and g2.dst = g5.src and g4.src = c3.dst
and g5.dst = c4.dst and c3.cnt < c4.cnt
```

### Query 6
```
SELECT * FROM Trade T1, Trade T2
WHERE T1.TT = "BUY" and T2.TT = "SALE"
and T1.CA_ID = T2.CA_ID
and T1.S_SYBM = T2.S_SYMB
and T1.T_DTS <= T2.T_DTS
and T1.T_DTS + interval '90' day >= T2.T_DTS
and T1.T_TRADE_PRICE*1.2 < T2.T_TRADE_PRICE
```

### Query 7
```
SELECT DISTINCT T1.T_CA_ID, T1.T_S_SYMB 
FROM Trade T1, Trade T2, Trade T3
WHERE T1.CA_ID = T2.CA_ID
and T1.S_SYMB = T2.S_SYMB
and T2.CA_ID = T3.CA_ID
and T2.S_SYMB = T3.S_SYMB
and T1.T_DTS + interval '90' day <= T2.T_DTS
and T2.T_DTS + interval '90' day <= T3.T_DTS
```

### Query 8
```
SELECT H1.CK, H2.CK, COUNT(DISTINCT H1.SK)
FROM Hold H1, Hold H2
WHERE H1.SK = H2. SK and H1.CK <> H2.CK
and H1.ST < H2.ET - interval '10' day
and H2.ST < H1.ET - interval '10' day
GROUP BY H1.CK, H2.CK
```

## Reproducibility of the experiments
### Pre-requisite
To run the experiment, we need to use the following tools. The version we are using is in parentheses.
Please make sure that these commands are available.
- readlink(readlink (GNU coreutils) 8.22)
- dirname(dirname (GNU coreutils) 8.22)
- getopt(getopt from util-linux 2.23.2)
- timeout(timeout (GNU coreutils) 8.22)
- tar(tar (GNU tar) 1.26)
- bc(bc 1.06.95)
- awk(GNU Awk 4.0.2)
- sed(sed (GNU sed) 4.2.2)
- java(openjdk version "1.8.0_342")
- scala(version 2.12.12)
- mvn(Apache Maven 3.8.4)
- gnuplot(gnuplot 5.0 patchlevel 0)

### Hareware Requirements
Recommended:
- Processors: 32 threads or above
- Memory: 400 GB or above
- Disk: 5 TB Space, 600 MB read/write speed or above 

All experiments reported were performed on a machine equipped with:
- Processors: 2 x Intel Xeon 2.1GHz processors each having 12 cores/24 threads
- Memory: 416 GB
- Disk Space: 4 x 4 TB HDD with RAID 5. 

To run in a machine with less memory, you need to manually change the `task*.*.executor.memory` and `task*.any_k.memory` in all the spec files(see the section `Spec Files` below). However, doing so may cause OOM or other exceptions in the execution. The execution time may be different due to the garbage collection.  

### Preparation
Please execute the following scripts or commands to prepare the environment for the experiment.
The following scripts or commands assume that your current working directory is the **ROOT DIRECTORY** of this project(the same directory as this README file). 
#### SparkCQC
Run the following script to compile SparkCQC
```shell
# This script will invoke 'mvn clean compile package'
# Check log/build.log if the build or compile procedure fails
bash experiments/build.sh
```
#### Graph data
Download the graph data from snap by running the script `experiments/data/create_graph_data.sh`
```shell
# This script will download the following graphs from snap(https://snap.stanford.edu)
# 1. bitcoin (from https://snap.stanford.edu/data/soc-sign-bitcoin-alpha.html)
# 2. epinions (from https://snap.stanford.edu/data/soc-Epinions1.html)
# 3. google (from https://snap.stanford.edu/data/web-Google.html)
# 4. wiki (from https://snap.stanford.edu/data/wiki-topcats.html)
# 5. dblp (from https://snap.stanford.edu/data/com-DBLP.html)
# and then convert them to a suitable format for further processing
bash experiments/data/create_graph_data.sh
```
#### TPC-E data
1. Download `tpc-e-tool.zip` from https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp
2. Move the downloaded `tpc-e-tool.zip` into `experiments/tpc_e` by a command like `mv /path/to/tpc-e-tool.zip experiments/tpc_e/tpc-e-tool.zip`
3. Run the following script to build the `tpc-e-tool`
```shell
# This script will unzip the tpc-e-tool.zip under experiments/tpc_e into experiments/tpc_e/tpc_e_tool/ and then build the tpc-e-tool
# It will print a message like 'tpc_e_tool build success' if the build completes
bash experiments/tpc_e/build_tpc_e_tool.sh
```
4. Run the following script to generate data by the `tpc-e-tool`
```shell
# This script will invoke the EGenLoader under the experiments/tpc_e/tpc_e_tool/bin/ with default configurations
# The raw data output path will be experiments/tpc_e/tpc_e_tool/flat_out/
# NOTE: it will take about 30 minutes to generate the data files
bash experiments/tpc_e/generate_tpc_e_data.sh
```
5. Run the following script to convert the tpc-e data to a suitable format
```shell
# This script will read the Trade.txt under experiments/tpc_e/tpc_e_tool/flat_out/ 
# and generates 5 data files(trade.txt, tradeB.txt, tradeS.txt, trade.in, holding.txt) in experiments/data/
# In tradeB.txt and the 'Relation TradeB' part of trade.in, the field T_TRADE_PRICE will be multiplied by 1.2
# for query Q6.
# NOTE: it will take about 30 minutes to generate the data files
bash experiments/data/create_tpc_e_data.sh
```
#### Spark
0. Change directory to any directory that you want to install your Spark
1. Download Spark 3.01 from https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
2. Extract the content
3. It is recommended to configure a large enough directory for `spark.local.dir` in `conf/spark-defaults.conf`.
```shell
tar -zxvf spark-3.0.1-bin-hadoop2.7.tgz
cd spark-3.0.1-bin-hadoop2.7
cp conf/spark-defaults.conf.template conf/spark-defaults.conf
# set spark.local.dir to a large enough tmp path in conf/spark-defaults.conf
# [NOTE] You should create this path beforehand
```
#### PostgreSQL Installation
0. Change directory to any directory that you want to install your PostgreSQL
1. Install PostgreSQL 14 according to the instructions on https://www.postgresql.org/docs/14/
2. Set 'max_worker_processes' to 16 in your `postgresql.conf` and re-start PostgreSQL
3. Create a database 'reproducibility'. You may use another name for the database.
4. Make sure you can access the database by 'psql -d {db_name} -U {your_name} -p {your_port}' (without a password)
#### Any-K
1. Change the working director back to the **ROOT DIRECTORY** of this project
2. run the following script to clone and build Any-K
```shell
# This script will clone Any-K from git@github.com:northeastern-datalab/anyk-code.git
# and then replace the class `experiments.Path_Unranked` with our version(experiments/any_k/Path_Unranked.java)
# as an implementation of the query in the Q6 experiment.
# Finally, it will call 'mvn package' to build a jar for Any-K.
bash experiments/any_k/build.sh 
```
#### Configurations
1. Create a `experiments/experimentsconfig.properties` file from template `experiments/config.properties.template`
```shell
cp experiments/config.properties.template experiments/config.properties
```
2. Set the configurations in `experiments/config.properties` based on your settings
```shell
# NOTES:
# 1. the common.tmp.path should be set to a path that is a large enough directory.
# During the experiments, it will write large files under this path to measure the I/O time

# 2. modify common.experiment.repeat, common.experiment.timeout, and common.experiment.postgresql.timeout
# if you need a quick result.
# See section 'Quick result' below for more details.
```
#### PostgreSQL Initialization
1. Make sure you have already created the graph data and tpc-e data in the previous steps
2. Run the following script to init your postgresql
```shell
# This script will create tables needed in the experiments and load data from experiments/data/
bash experiments/postgresql/init.sh
```
#### Graph data preparation
Before running parallel experiments, the graph data need pre-processing. Run the following script
to pre-process the graph data.
```shell
# The following script will submit a job to your spark cluster. The job will read the graph data
# and write it out as 32 part-* files.
bash experiments/data/prepare_graph_data.sh
```
#### Final check
You may run the following script to check if you have set up the environment correctly.
```shell
# This script will check the following:
# 1. SparkCQC has been compiled
# 2. the graph data files exist
# 3. the tpc-e tool files exist
# 4. the tpc-e data files exist
# 5. the spark.home is set properly
# 6. the prepared versions of the graphs exist
# 7. PostgreSQL server is running, and command psql can connect with the server
# 8. the tables in PostgreSQL are all created and non-empty
# 9. Any-K has been compiled
bash experiments/check.sh
```

### Execution
#### Quick result
Typically, the experiments take a long time to run.

You can set the value of `common.experiment.repeat`(default value=10, means repeat 10 times)
in `config.properties` to less than 10(even 1) if you want a quick result.
However, each iteration may still take several days.
Also, due to the variations in a single experiment, the result figures may be slightly different
from those in the paper.

You may also configure the value of `common.experiment.timeout`, which is used to control the
maximum timeout for each task. This value is passed to the timeout command as the DURATION parameter,
so any legal DURATION parameter (e.g., 4h) is acceptable.
Reducing `common.experiment.timeout` can drastically reduce the time needed to run all experiments,
but it may also cause the lack of data for some time-consuming tasks in the resulting figure.

The value of `common.experiment.postgresql.timeout`(in milliseconds) should be consistent
with `common.experiment.timeout`. This configuration will be passed to postgresql as
'statement_timeout' in execution. Please remember to set this value if you have changed the `common.experiment.timeout` value.

#### Spec files
There are some `*.spec` files under the folder `experiments/specs/`. Each of them corresponds to an experiment in the paper.
For example, the `experiments/specs/parallel_processing/q2_bitcoin/q2_bitcoin.spec` corresponds to the parallel experiment of Q2.

Each spec file is composed of several tasks, and each task corresponds to a data point in the figure.
For example, the `task10` in the aforementioned `q2_bitcoin.spec` measures the execution time of SparkCQC
in the parallel experiment of Q2 under configuration `parallelism = 4`.
#### Execute script
Use the `experiments/execute.sh` script to execute the spec files.
```shell
Usage: execute.sh [Options]

Options:
  -h, --help                Print this message.
  -a, --all                 Run all the spec files.
  -s, --spec <spec file>    Run the specific spec file. (will be ignored if '-a' is set)
  -t, --task <task ID>      Run the specific task only. (will be ignored if '-a' is set)

Examples:
  (1) bash execute.sh -a
        Run all the spec files.

  (2) bash execute.sh -s specs/parallel_processing/q2_bitcoin/q2_bitcoin.spec
        Run the q2_bitcoin.spec in a parallel experiment.

  (3) bash execute.sh -s specs/parallel_processing/q2_bitcoin/q2_bitcoin.spec -t 10
        Run only the task10 in q2_bitcoin.spec in a parallel experiment.
```
To run all the experiments, just run the execute.sh script with '-a' and wait for its termination.
```shell
bash experiments/execute.sh -a &
```
#### Result
Each spec file has a `spec.experiment.name` configuration. All the execution results(in milliseconds). All the execution results(in milliseconds)
are stored at the path `experiments/output/result/{spec.experiment.name}/{task_name}.txt`. The result of the aforementioned `q2_bitcoin.spec` will be stored at `experiments/output/result/parallel_q2_bitcoin/task10.txt`. The script will write a '-1' in the result file for those failed or timed out executions.

### Plotting
#### Plot files
There are 4 folders under the path `experiments/plot/`. They correspond to the experiment results(Figure 7-10) in the paper.
For example, the `experiments/plot/parallel_processing/` corresponds to 'Figure 9: Processing times under different parallelism'.
You can run the `plot.sh` script in the
corresponding folder if you need to plot only a particular figure. In most cases, you can use the `plot_all.sh` script in the experiments
directory (see the section `Plot all` below) to plot all the figures at once.

All the scripts `*/plot.sh` under the `experiments/plot/` folder will compute the average execution time by the result files
in execution. Then it will generate a `*.avg` file with the same name to store the average value.
For example, the average value of the rows in `task10.txt` will produce a `task10.avg` at the same path.

After that, the script will generate some `*.dat` files by combining the `*.avg` files. The `dat` files are then
sent to the `gnuplot` command for plotting.

For example, the script `experiments/plot/parallel_processing/q2_bitcoin/plot.sh` will compute the average for all the `*.txt`
under `experiments/output/result/parallel_q2_bitcoin/` and generate the `*.avg` files. Then these files are combined in `io.dat`,
`sparkcqc.dat`, `sparksql.dat`, and `postgresql.dat` at the path `experiments/output/result/parallel_q2_bitcoin`. Finally, it
will plot the 'Q2-Bitcoin' part of Figure 9 by command gnuplot.
#### Plot all
You can run the script `plot_all.sh` to plot all the figures.
```shell
# This script will plot the experiment results(Figure 7-10).
# For Fig 7, the output path is experiments/output/figure/running_time/result.png
# For Fig 8, the output path is experiments/output/figure/selectivity/*.png
# For Fig 9, the output path is experiments/output/figure/parallel_processing/*.png
# For Fig 10, the output path is experiments/output/figure/different_systems/result.png
bash experiments/plot_all.sh
```

### Troubleshooting
1. Run the check script (see the 'Final check' section above) to ensure you have configured your
   environment correctly.
2. Check the log files under the `experiments/log/` path. For example, if the SparkSQL experiment terminates quickly in the experiment of parallel_q2_bitcoin, you may check the `experiments/log/execute_parallel_q2_bitcoin.log` to find out what is the problem in the previous execution. You may search for the keyword 'executing task: x' to jump to the beginning of the problematic task.
