spec.experiment.name=q10
spec.tasks.count=15

# io common properties
task.io.classname=org.SparkCQC.Query10SparkCQC
task.io.cores.max=1
task.io.default.parallelism=2
task.io.driver.memory=16G
task.io.executor.cores=1
task.io.executor.memory=360G
task.io.k.value=0

# cqc common properties
task.cqc.cores.max=1
task.cqc.default.parallelism=2
task.cqc.driver.memory=16G
task.cqc.executor.cores=1
task.cqc.executor.memory=360G
task.cqc.k.value=0

# spark common properties
task.spark.classname=org.SparkCQC.Query10SparkSQL
task.spark.cores.max=1
task.spark.default.parallelism=2
task.spark.driver.memory=16G
task.spark.executor.cores=1
task.spark.executor.memory=360G
task.spark.k.value=0

# postgresql common properties
task.postgresql.parallelism=1

# task specific properties
# Q10-Epinions
task1.system.name=io
task1.io.graph.name=epinions.txt

task2.system.name=cqc
task2.cqc.graph.name=epinions.txt
task2.cqc.classname=org.SparkCQC.Query10SparkCQC

task3.system.name=cqc
task3.cqc.graph.name=epinions.txt
task3.cqc.classname=org.SparkCQC.Query10TBSparkCQC

task4.system.name=spark
task4.spark.graph.name=epinions.txt

task5.system.name=postgresql
task5.postgresql.query=specs/graph_queries/q10/q10_epinions_postgresql.sql

# Q10-Google
task6.system.name=io
task6.io.graph.name=google.txt

task7.system.name=cqc
task7.cqc.graph.name=google.txt
task7.cqc.classname=org.SparkCQC.Query10SparkCQC

task8.system.name=cqc
task8.cqc.graph.name=google.txt
task8.cqc.classname=org.SparkCQC.Query10TBSparkCQC

task9.system.name=spark
task9.spark.graph.name=google.txt

task10.system.name=postgresql
task10.postgresql.query=specs/graph_queries/q10/q10_google_postgresql.sql

# Q10-Wiki
task11.system.name=io_huge
task11.io_huge.classname=org.SparkCQC.Query10SparkCQC
task11.io_huge.cores.max=16
task11.io_huge.default.parallelism=32
task11.io_huge.executor.cores=16
task11.io_huge.graph.name=wiki-txt-prepared
task11.io_huge.driver.memory=16G
task11.io_huge.executor.memory=360G
task11.io_huge.k.value=0

task12.system.name=cqc
task12.cqc.classname=org.SparkCQC.Query10SparkCQC
task12.cqc.cores.max=16
task12.cqc.default.parallelism=32
task12.cqc.executor.cores=16
task12.cqc.graph.name=wiki-txt-prepared

task13.system.name=cqc
task13.cqc.classname=org.SparkCQC.Query10TBSparkCQC
task13.cqc.cores.max=16
task13.cqc.default.parallelism=32
task13.cqc.executor.cores=16
task13.cqc.graph.name=wiki-txt-prepared

task14.system.name=spark
task14.spark.cores.max=16
task14.spark.default.parallelism=32
task14.spark.executor.cores=16
task14.spark.graph.name=wiki-txt-prepared

task15.system.name=postgresql
task15.postgresql.parallelism=16
task15.postgresql.query=specs/graph_queries/q10/q10_wiki_postgresql.sql