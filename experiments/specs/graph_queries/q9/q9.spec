spec.experiment.name=q9
spec.tasks.count=12

# io common properties
task.io.classname=org.SparkCQC.Query9SparkCQC
task.io.cores.max=1
task.io.default.parallelism=2
task.io.driver.memory=16G
task.io.executor.cores=1
task.io.executor.memory=360G
task.io.k.value=0

# cqc common properties
task.cqc.classname=org.SparkCQC.Query9SparkCQC
task.cqc.cores.max=1
task.cqc.default.parallelism=2
task.cqc.driver.memory=16G
task.cqc.executor.cores=1
task.cqc.executor.memory=360G
task.cqc.k.value=0

# spark common properties
task.spark.classname=org.SparkCQC.Query9SparkSQL
task.spark.cores.max=1
task.spark.default.parallelism=2
task.spark.driver.memory=16G
task.spark.executor.cores=1
task.spark.executor.memory=360G
task.spark.k.value=0

# postgresql common properties
task.postgresql.parallelism=1

# task specific properties
# Q9-Epinions
task1.system.name=io
task1.io.graph.name=epinions.txt

task2.system.name=cqc
task2.cqc.graph.name=epinions.txt

task3.system.name=spark
task3.spark.graph.name=epinions.txt

task4.system.name=postgresql
task4.postgresql.query=specs/graph_queries/q9/q9_epinions_postgresql.sql

# Q9-Google
task5.system.name=io
task5.io.graph.name=google.txt
task5.io.k.value=500

task6.system.name=cqc
task6.cqc.graph.name=google.txt
task6.cqc.k.value=500

task7.system.name=spark
task7.spark.graph.name=google.txt
task7.spark.k.value=500

task8.system.name=postgresql
task8.postgresql.query=specs/graph_queries/q9/q9_google_postgresql.sql

# Q9-Wiki
task9.system.name=io_huge
task9.io_huge.classname=org.SparkCQC.Query9SparkCQC
task9.io_huge.cores.max=16
task9.io_huge.default.parallelism=32
task9.io_huge.executor.cores=16
task9.io_huge.graph.name=wiki-txt-prepared
task9.io_huge.driver.memory=16G
task9.io_huge.executor.memory=360G
task9.io_huge.k.value=1000

task10.system.name=cqc
task10.cqc.cores.max=16
task10.cqc.default.parallelism=32
task10.cqc.executor.cores=16
task10.cqc.graph.name=wiki-txt-prepared
task10.cqc.k.value=1000

task11.system.name=spark
task11.spark.cores.max=16
task11.spark.default.parallelism=32
task11.spark.executor.cores=16
task11.spark.graph.name=wiki-txt-prepared
task11.cqc.k.value=1000

task12.system.name=postgresql
task12.postgresql.parallelism=16
task12.postgresql.query=specs/graph_queries/q9/q9_wiki_postgresql.sql
