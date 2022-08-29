spec.experiment.name=selectivity_q1_epinions
spec.tasks.count=20

# io common properties
task.io.classname=org.SparkCQC.Query1SparkCQC
task.io.driver.memory=16G
task.io.executor.memory=360G
task.io.graph.name=epinions.txt
task.io.cores.max=1
task.io.default.parallelism=2
task.io.executor.cores=1

# cqc common properties
task.cqc.classname=org.SparkCQC.Query1SparkCQC
task.cqc.driver.memory=16G
task.cqc.executor.memory=360G
task.cqc.graph.name=epinions.txt
task.cqc.cores.max=1
task.cqc.default.parallelism=2
task.cqc.executor.cores=1

# spark common properties
task.spark.classname=org.SparkCQC.Query1SparkSQL
task.spark.driver.memory=16G
task.spark.executor.memory=360G
task.spark.graph.name=epinions.txt
task.spark.cores.max=1
task.spark.default.parallelism=2
task.spark.executor.cores=1

# postgresql common properties
task.postgresql.parallelism=1

# task specific properties
# k = 1000
task1.system.name=io
task1.io.k.value=1000

task2.system.name=cqc
task2.cqc.k.value=1000

task3.system.name=spark
task3.spark.k.value=1000

task4.system.name=postgresql
task4.postgresql.query=specs/selectivity/q1_epinions/q1_epinions_1000_postgresql.sql

# k = 100
task5.system.name=io
task5.io.k.value=100

task6.system.name=cqc
task6.cqc.k.value=100

task7.system.name=spark
task7.spark.k.value=100

task8.system.name=postgresql
task8.postgresql.query=specs/selectivity/q1_epinions/q1_epinions_100_postgresql.sql

# k = 50
task9.system.name=io
task9.io.k.value=50

task10.system.name=cqc
task10.cqc.k.value=50

task11.system.name=spark
task11.spark.k.value=50

task12.system.name=postgresql
task12.postgresql.query=specs/selectivity/q1_epinions/q1_epinions_50_postgresql.sql

# k = 10
task13.system.name=io
task13.io.k.value=10

task14.system.name=cqc
task14.cqc.k.value=10

task15.system.name=spark
task15.spark.k.value=10

task16.system.name=postgresql
task16.postgresql.query=specs/selectivity/q1_epinions/q1_epinions_10_postgresql.sql

# k = 0
task17.system.name=io
task17.io.k.value=0

task18.system.name=cqc
task18.cqc.k.value=0

task19.system.name=spark
task19.spark.k.value=0

task20.system.name=postgresql
task20.postgresql.query=specs/selectivity/q1_epinions/q1_epinions_0_postgresql.sql
