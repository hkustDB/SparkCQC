spec.experiment.name=q3
spec.tasks.count=12

# io common properties
task.io.classname=org.SparkCQC.Query3SparkCQCPlan1
task.io.cores.max=1
task.io.default.parallelism=2
task.io.driver.memory=16G
task.io.executor.cores=1
task.io.executor.memory=360G
task.io.k.value=0

# cqc common properties
task.cqc.classname=org.SparkCQC.Query3SparkCQCPlan1
task.cqc.cores.max=1
task.cqc.default.parallelism=2
task.cqc.driver.memory=16G
task.cqc.executor.cores=1
task.cqc.executor.memory=360G
task.cqc.k.value=0

# spark common properties
task.spark.classname=org.SparkCQC.Query3SparkSQL
task.spark.cores.max=1
task.spark.default.parallelism=2
task.spark.driver.memory=16G
task.spark.executor.cores=1
task.spark.executor.memory=360G
task.spark.k.value=0

# postgresql common properties
task.postgresql.parallelism=1

# task specific properties
# Q3-Epinions
task1.system.name=io
task1.io.graph.name=epinions.txt

task2.system.name=cqc
task2.cqc.graph.name=epinions.txt

task3.system.name=spark
task3.spark.graph.name=epinions.txt

task4.system.name=postgresql
task4.postgresql.query=specs/graph_queries/q3/q3_epinions_postgresql.sql

# Q3-Google
task5.system.name=io
task5.io.graph.name=google.txt

task6.system.name=cqc
task6.cqc.graph.name=google.txt

task7.system.name=spark
task7.spark.graph.name=google.txt

task8.system.name=postgresql
task8.postgresql.query=specs/graph_queries/q3/q3_google_postgresql.sql

# Q3-Wiki
task9.system.name=io
task9.io.cores.max=16
task9.io.default.parallelism=32
task9.io.executor.cores=16
task9.io.graph.name=wiki.txt

task10.system.name=cqc
task10.cqc.cores.max=16
task10.cqc.default.parallelism=32
task10.cqc.executor.cores=16
task10.cqc.graph.name=wiki.txt

task11.system.name=spark
task11.spark.cores.max=16
task11.spark.default.parallelism=32
task11.spark.executor.cores=16
task11.spark.graph.name=wiki.txt

task12.system.name=postgresql
task12.postgresql.parallelism=16
task12.postgresql.query=specs/graph_queries/q3/q3_wiki_postgresql.sql
