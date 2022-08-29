spec.experiment.name=q5
spec.tasks.count=8

# io common properties
task.io.classname=org.SparkCQC.Query5SparkCQC
task.io.cores.max=1
task.io.default.parallelism=2
task.io.driver.memory=16G
task.io.executor.cores=1
task.io.executor.memory=360G
task.io.k.value=0

# cqc common properties
task.cqc.classname=org.SparkCQC.Query5SparkCQC
task.cqc.cores.max=1
task.cqc.default.parallelism=2
task.cqc.driver.memory=16G
task.cqc.executor.cores=1
task.cqc.executor.memory=360G
task.cqc.k.value=0

# spark common properties
task.spark.classname=org.SparkCQC.Query5SparkSQL
task.spark.cores.max=1
task.spark.default.parallelism=2
task.spark.driver.memory=16G
task.spark.executor.cores=1
task.spark.executor.memory=360G
task.spark.k.value=0

# postgresql common properties
task.postgresql.parallelism=1

# task specific properties
# Q5-DBLP
task1.system.name=io
task1.io.graph.name=dblp.txt

task2.system.name=cqc
task2.cqc.graph.name=dblp.txt

task3.system.name=spark
task3.spark.graph.name=dblp.txt

task4.system.name=postgresql
task4.postgresql.query=specs/graph_queries/q5/q5_dblp_postgresql.sql

# Q5-Google
task5.system.name=io
task5.io.graph.name=google.txt

task6.system.name=cqc
task6.cqc.graph.name=google.txt

task7.system.name=spark
task7.spark.graph.name=google.txt

task8.system.name=postgresql
task8.postgresql.query=specs/graph_queries/q5/q5_google_postgresql.sql
