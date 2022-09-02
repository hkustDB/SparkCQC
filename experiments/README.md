### Pre-requisite
To run the experiment, we need to use the following tools. The version we are using is in parentheses. 
Please make sure that these commands are in the environment variable PATH.
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

### Preparation
#### SparkCQC
Run the following script to compile SparkCQC
```shell
# This script will change directory to '../SparkCQC' and invoke 'mvn clean compile package'
# Check log/build.log if build or compile fail
bash build.sh
```
#### Graph data
Download the graph data from snap by running the script `data/create_graph_data.sh`
```shell
# This script will download following graphs from snap(https://snap.stanford.edu)
# 1. bitcoin (from https://snap.stanford.edu/data/soc-sign-bitcoin-alpha.html)
# 2. epinions (from https://snap.stanford.edu/data/soc-Epinions1.html)
# 3. google (from https://snap.stanford.edu/data/web-Google.html)
# 4. wiki (from https://snap.stanford.edu/data/wiki-topcats.html)
# 5. dblp (from https://snap.stanford.edu/data/com-DBLP.html)
# and then convert them to a suitable format for further processing
bash data/create_graph_data.sh
```
#### TPC-E data
1. Download `tpc-e-tool.zip` from https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp
2. Move the downloaded `tpc-e-tool.zip` into `tpc_e` by command like `mv /path/to/tpc-e-tool.zip tpc_e/tpc-e-tool.zip`
3. Run the following script to build the `tpc-e-tool`
```shell
# This script will unzip the tpc-e-tool.zip under tpc_e into tpc_e/tpc_e_tool/ and then build the tpc-e-tool
# It will print a message like 'tpc_e_tool build success' if the build is completed
bash tpc_e/build_tpc_e_tool.sh
```
4. Run the following script to generate data by the `tpc-e-tool`
```shell
# This script will invoke the EGenLoader under the tpc_e/tpc_e_tool/bin/ with default configurations
# The raw data output path will be tpc_e/tpc_e_tool/flat_out/
# NOTE: it will take about 30 minutes to generate the data files
bash tpc_e/generate_tpc_e_data.sh
```
5. Run the following script to convert the tpc-e data to suitable format
```shell
# This script will read the Trade.txt under tpc_e/tpc_e_tool/flat_out/ 
# and generates 5 data files(trade.txt, tradeB.txt, tradeS.txt, trade.in, holding.txt) in data/
# In tradeB.txt and the 'Relation TradeB' part of trade.in, the field T_TRADE_PRICE will be multiplied by 1.2
# for query Q6.
# NOTE: it will take about 30 minutes to generate the data files
bash data/create_tpc_e_data.sh
```
#### Spark
1. Download Spark 3.01 from https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
2. It is recommended to configure a large enough free directory for `spark.local.dir` in `conf/spark-defaults.conf`.
3. Extract the content and start a local Spark cluster
```shell
tar -zxvf spark-3.0.1-bin-hadoop2.7.tgz
cd spark-3.0.1-bin-hadoop2.7
cp conf/spark-defaults.conf.template conf/spark-defaults.conf
# set spark.local.dir to a large enough tmp path in conf/spark-defaults.conf
# [NOTE] this path should be created beforehand
bash sbin/start-all.sh
```
#### PostgreSQL Installation
1. Install PostgreSQL 14 according to the instructions on https://www.postgresql.org/docs/14/
2. Set 'max_worker_processes' to 16 in your `postgresql.conf` and re-start PostgreSQL
3. Create a database 'reproducibility'. You may use another name for the database.
4. Make sure you can access the database by 'psql -d {db_name} -U {your_name} -p {your_port}' (without a password)
#### Any-K
run the following script to clone and build Any-K
```shell
# This script will clone Any-K from git@github.com:northeastern-datalab/anyk-code.git
# and then replace the class `experiments.Path_Unranked` with our version(any_k/Path_Unranked.java)
# as an implementation of the query in Q6 experiment.
# Finally, it will call 'mvn package' to build a jar for Any-K.
bash any_k/build.sh 
```
#### Configurations
1. Create a `config.properties` file from template `config.properties.template`
```shell
cp config.properties.template config.properties
```
2. Set the configurations in `config.properties` based on your settings
```shell
# NOTES:
# 1. the common.tmp.path should be set to a path that is large enough. 
# During the experiments, it will write large files under this path to measure the I/O time

# 2. modify common.experiment.repeat and common.experiment.timeout if you need a quick result.
# See section 'Quick result' below for more details.
```
#### PostgreSQL Initialization
1. Make sure you have already created the graph data and tpc-e data in the previous steps
2. Run the following script to init your postgresql
```shell
# This script will create tables needed in the experiments and load data from data/
bash postgresql/init.sh
```
#### Graph data preparation
Before running parallel experiments, the graph data need pre-processing. Run the following script
to pre-process the graph data.
```shell
# The following script will submit a job to your spark cluster. The job will read the graph data
# and write it out with 32 partitions.
bash data/prepare_graph_data.sh
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
# 7. PostgreSQL server is running and command psql is able to connect with the server
# 8. the tables in PostgreSQL is all created and non-empty
# 9. Any-K has been compiled
bash check.sh
```

### Execution
#### Quick result
Typically, the experiments take a long time to run.

You can set the value of `common.experiment.repeat`(default value=10, means repeat 10 times)
in `config.properties` to less than 10(even 1) if you want a quick result. 
However, each iteration may still take several days. 
Also, due to the variations in single experiment, the result figures may be slightly different
from those in the paper.

You may also configure the value of `common.experiment.timeout`, which is used to control the
maximum timeout for each task. This value is passed to the timeout command as the DURATION parameter,
so any legal DURATION parameter (e.g. 4h) is acceptable. 
Reducing `common.experiment.timeout` can drastically reduce the time needed to run all experiments,
but it may also cause the lack of data for some difficult tasks in the result figure.

The value of `common.experiment.postgresql.timeout`(in milliseconds) should be consistent
with `common.experiment.timeout`. This configuration will be pass to postgresql as
'statement_timeout' in execution. Please remember to set this value in case you have changed the value
of `common.experiment.timeout`.

#### Spec files
There are some `*.spec` files under the folder `specs/`. Each of them corresponds to an experiment in the paper.
For example, the `specs/parallel_processing/q2_bitcoin/q2_bitcoin.spec` corresponds the parallel experiment of Q2.

Each spec file is composed of several tasks, and each task corresponds to a data point in the figure.
For example, the `task10` in the aforementioned `q2_bitcoin.spec` measures the execution time of SparkCQC
in the parallel experiment of Q2 under configuration `parallelism = 4`.
#### Execute script
Use the `execute.sh` under the root path to execute the spec files.
```shell
Usage: execute.sh [Options]

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
```
To run all the experiments, just run the execute.sh script with '-a' and wait. 
```shell
bash execute.sh -a &
```
#### Result
There is a `spec.experiment.name` configuration in each of the spec files. All the execution results(in milliseconds) 
are stored at the path `output/result/{spec.experiment.name}/{task_name}.txt`. For example, the results of `task10` 
in the aforementioned `q2_bitcoin.spec` will be stored at `output/result/parallel_q2_bitcoin/task10.txt`. For those 
failed or timed out executions, a '-1' will be written in the result file. 

### Plotting
#### Plot files
There are 4 folders under the path `plot/`. They correspond to the experiment results(Figure 7-10) in the paper. 
For example, the `plot/parallel_processing/` corresponds to 'Figure 9: Processing times under different parallelism'.
If you need to plot only a particular figure, you can run the `plot.sh `script at the
corresponding folder. In most cases, you can use the `plot_all.sh` script in the root
directory (see the section `Plot all` below) to plot all the figures at once.

All the scripts `*/plot.sh` under the `plot/` folder will compute the average execution time by the result files 
in execution. Then it will generate a `*.avg` file with the same name to store the average value. 
For example, the average value of the rows in `task10.txt` will produce a `task10.avg` at the same path.

After that, the script will generate some `*.dat` files by combining the `*.avg` files. The `dat` files are then 
sent to the `gnuplot` command for plotting.

For example, the script `plot/parallel_processing/q2_bitcoin/plot.sh` will compute the average for all the `*.txt`
under `output/result/parallel_q2_bitcoin/` and generate the `*.avg` files. Then these files are combined in `io.dat`, 
`sparkcqc.dat`, `sparksql.dat`, and `postgresql.dat` at the path `output/result/parallel_q2_bitcoin`. Finally, it 
will plot the 'Q2-Bitcoin' part of Figure 9 by command gnuplot.
#### Plot all
You can run the script `plot_all.sh` to plot all the figures. 
```shell
# This script will plot the experiment results(Figure 7-10).
# For Fig 7, the output path is output/figure/running_time/result.png
# For Fig 8, the output path is output/figure/selectivity/*.png
# For Fig 9, the output path is output/figure/parallel_processing/*.png
# For Fig 10, the output path is output/figure/different_systems/result.png
bash plot_all.sh
```

### Troubleshooting
1. Run the check script (see the 'Final check' section above) to make sure you have configured your 
   environment correctly.
2. Check the log files under the `log/` path. For example, if the SparkSQL experiment terminates quickly in
   the experiment of parallel_q2_bitcoin, you may check the `log/execute_parallel_q2_bitcoin.log` to find out
   what is the problem in the previous execution. You may search for the keyword 'executing task: x' to jump to
   the beginning of the problematic task.
