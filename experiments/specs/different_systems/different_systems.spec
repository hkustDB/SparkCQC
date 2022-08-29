spec.experiment.name=different_systems
spec.tasks.count=6

# task specific properties
task1.system.name=io
task1.io.classname=org.SparkCQC.Query6SparkCQC
task1.io.cores.max=1
task1.io.default.parallelism=2
task1.io.driver.memory=16G
task1.io.executor.cores=1
task1.io.executor.memory=360G
task1.io.graph.name=dummy
task1.io.k.value=0

task2.system.name=cqc
task2.cqc.classname=org.SparkCQC.Query6SparkCQC
task2.cqc.cores.max=1
task2.cqc.default.parallelism=2
task2.cqc.driver.memory=16G
task2.cqc.executor.cores=1
task2.cqc.executor.memory=360G
task2.cqc.graph.name=dummy
task2.cqc.k.value=0

task3.system.name=spark
task3.spark.classname=org.SparkCQC.Query6SparkSQL
task3.spark.cores.max=1
task3.spark.default.parallelism=2
task3.spark.driver.memory=16G
task3.spark.executor.cores=1
task3.spark.executor.memory=360G
task3.spark.graph.name=trade.txt
task3.spark.k.value=0

task4.system.name=postgresql
task4.postgresql.parallelism=1
task4.postgresql.query=specs/analytical_queries/q6/q6_postgresql.sql

# Willard's Approach
task5.system.name=cqc
task5.cqc.classname=org.SparkCQC.Query6Comparison
task5.cqc.cores.max=1
task5.cqc.default.parallelism=2
task5.cqc.driver.memory=16G
task5.cqc.executor.cores=1
task5.cqc.executor.memory=360G
task5.cqc.graph.name=dummy
task5.cqc.k.value=0

# Any-K
task6.system.name=any_k
task6.any_k.classname=experiments.Path_Unranked
task6.any_k.in.file=trade.in
task6.any_k.memory=360G