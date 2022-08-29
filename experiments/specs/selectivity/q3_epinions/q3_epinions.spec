spec.experiment.name=selectivity_q3_epinions
spec.tasks.count=25

# io common properties
task.io.classname=org.SparkCQC.Query3SparkCQCPlan1
task.io.driver.memory=16G
task.io.executor.memory=360G
task.io.graph.name=epinions.txt
task.io.cores.max=1
task.io.default.parallelism=2
task.io.executor.cores=1

# cqc common properties
task.cqc.driver.memory=16G
task.cqc.executor.memory=360G
task.cqc.graph.name=epinions.txt
task.cqc.cores.max=1
task.cqc.default.parallelism=2
task.cqc.executor.cores=1

# spark common properties
task.spark.classname=org.SparkCQC.Query3SparkSQL
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
task2.cqc.classname=org.SparkCQC.Query3SparkCQCPlan1
task2.cqc.k.value=1000

task3.system.name=cqc
task3.cqc.classname=org.SparkCQC.Query3SparkCQCPlan2
task3.cqc.k.value=1000

task4.system.name=spark
task4.spark.k.value=1000

task5.system.name=postgresql
task5.postgresql.query=specs/selectivity/q3_epinions/q3_epinions_1000_postgresql.sql

# k = 100
task6.system.name=io
task6.io.k.value=100

task7.system.name=cqc
task7.cqc.classname=org.SparkCQC.Query3SparkCQCPlan1
task7.cqc.k.value=100

task8.system.name=cqc
task8.cqc.classname=org.SparkCQC.Query3SparkCQCPlan2
task8.cqc.k.value=100

task9.system.name=spark
task9.spark.k.value=100

task10.system.name=postgresql
task10.postgresql.query=specs/selectivity/q3_epinions/q3_epinions_100_postgresql.sql

# k = 50
task11.system.name=io
task11.io.k.value=50

task12.system.name=cqc
task12.cqc.classname=org.SparkCQC.Query3SparkCQCPlan1
task12.cqc.k.value=50

task13.system.name=cqc
task13.cqc.classname=org.SparkCQC.Query3SparkCQCPlan2
task13.cqc.k.value=50

task14.system.name=spark
task14.spark.k.value=50

task15.system.name=postgresql
task15.postgresql.query=specs/selectivity/q3_epinions/q3_epinions_50_postgresql.sql

# k = 10
task16.system.name=io
task16.io.k.value=10

task17.system.name=cqc
task17.cqc.classname=org.SparkCQC.Query3SparkCQCPlan1
task17.cqc.k.value=10

task18.system.name=cqc
task18.cqc.classname=org.SparkCQC.Query3SparkCQCPlan2
task18.cqc.k.value=10

task19.system.name=spark
task19.spark.k.value=10

task20.system.name=postgresql
task20.postgresql.query=specs/selectivity/q3_epinions/q3_epinions_10_postgresql.sql

# k = 0
task21.system.name=io
task21.io.k.value=0

task22.system.name=cqc
task22.cqc.classname=org.SparkCQC.Query3SparkCQCPlan1
task22.cqc.k.value=0

task23.system.name=cqc
task23.cqc.classname=org.SparkCQC.Query3SparkCQCPlan2
task23.cqc.k.value=0

task24.system.name=spark
task24.spark.k.value=0

task25.system.name=postgresql
task25.postgresql.query=specs/selectivity/q3_epinions/q3_epinions_0_postgresql.sql
