spec.experiment.name=q11
spec.tasks.count=4

# io common properties
task.io.classname=org.SparkCQC.Query11SparkCQC
task.io.cores.max=1
task.io.default.parallelism=2
task.io.driver.memory=16G
task.io.executor.cores=1
task.io.executor.memory=360G
task.io.k.value=0

# cqc common properties
task.cqc.classname=org.SparkCQC.Query11SparkCQC
task.cqc.cores.max=1
task.cqc.default.parallelism=2
task.cqc.driver.memory=16G
task.cqc.executor.cores=1
task.cqc.executor.memory=360G
task.cqc.k.value=0

# spark common properties
task.spark.classname=org.SparkCQC.Query11SparkSQL
task.spark.cores.max=1
task.spark.default.parallelism=2
task.spark.driver.memory=16G
task.spark.executor.cores=1
task.spark.executor.memory=360G
task.spark.k.value=0

# postgresql common properties
task.postgresql.parallelism=1

# task specific properties
task1.system.name=io
task1.io.graph.name=dummy

task2.system.name=cqc
task2.cqc.graph.name=dummy

task3.system.name=spark
task3.spark.graph.name=trade.txt

task4.system.name=postgresql
task4.postgresql.query=specs/analytical_queries/q11/q11_postgresql.sql
