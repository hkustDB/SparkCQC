spec.experiment.name=parallel_q1_epinions
spec.tasks.count=20

# io common properties
task.io.classname=org.SparkCQC.Query1SparkCQC
task.io.driver.memory=16G
task.io.executor.memory=360G
task.io.graph.name=epinions-txt-prepared
task.io.k.value=0

# cqc common properties
task.cqc.classname=org.SparkCQC.Query1SparkCQC
task.cqc.driver.memory=16G
task.cqc.executor.memory=360G
task.cqc.graph.name=epinions-txt-prepared
task.cqc.k.value=0

# spark common properties
task.spark.classname=org.SparkCQC.Query1SparkSQL
task.spark.driver.memory=16G
task.spark.executor.memory=360G
task.spark.graph.name=epinions-txt-prepared
task.spark.k.value=0

# postgresql common properties
task.postgresql.query=specs/parallel_processing/q1_epinions/q1_epinions_postgresql.sql

# task specific properties
# parallelism = 1
task1.system.name=io
task1.io.cores.max=1
task1.io.default.parallelism=2
task1.io.executor.cores=1

task2.system.name=cqc
task2.cqc.cores.max=1
task2.cqc.default.parallelism=2
task2.cqc.executor.cores=1

task3.system.name=spark
task3.spark.cores.max=1
task3.spark.default.parallelism=2
task3.spark.executor.cores=1

task4.system.name=postgresql
task4.postgresql.parallelism=1

# parallelism = 2
task5.system.name=io
task5.io.cores.max=2
task5.io.default.parallelism=4
task5.io.executor.cores=2

task6.system.name=cqc
task6.cqc.cores.max=2
task6.cqc.default.parallelism=4
task6.cqc.executor.cores=2

task7.system.name=spark
task7.spark.cores.max=2
task7.spark.default.parallelism=4
task7.spark.executor.cores=2

task8.system.name=postgresql
task8.postgresql.parallelism=2

# parallelism = 4
task9.system.name=io
task9.io.cores.max=4
task9.io.default.parallelism=8
task9.io.executor.cores=4

task10.system.name=cqc
task10.cqc.cores.max=4
task10.cqc.default.parallelism=8
task10.cqc.executor.cores=4

task11.system.name=spark
task11.spark.cores.max=4
task11.spark.default.parallelism=8
task11.spark.executor.cores=4

task12.system.name=postgresql
task12.postgresql.parallelism=4

# parallelism = 8
task13.system.name=io
task13.io.cores.max=8
task13.io.default.parallelism=16
task13.io.executor.cores=8

task14.system.name=cqc
task14.cqc.cores.max=8
task14.cqc.default.parallelism=16
task14.cqc.executor.cores=8

task15.system.name=spark
task15.spark.cores.max=8
task15.spark.default.parallelism=16
task15.spark.executor.cores=8

task16.system.name=postgresql
task16.postgresql.parallelism=8

# parallelism = 16
task17.system.name=io
task17.io.cores.max=16
task17.io.default.parallelism=32
task17.io.executor.cores=16

task18.system.name=cqc
task18.cqc.cores.max=16
task18.cqc.default.parallelism=32
task18.cqc.executor.cores=16

task19.system.name=spark
task19.spark.cores.max=16
task19.spark.default.parallelism=32
task19.spark.executor.cores=16

task20.system.name=postgresql
task20.postgresql.parallelism=16
