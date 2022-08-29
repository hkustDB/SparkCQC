spec.experiment.name=parallel_q3_epinions
spec.tasks.count=25

# io common properties
task.io.classname=org.SparkCQC.Query3SparkCQCPlan1
task.io.driver.memory=16G
task.io.executor.memory=360G
task.io.graph.name=epinions-txt-prepared
task.io.k.value=0

# cqc common properties
task.cqc.driver.memory=16G
task.cqc.executor.memory=360G
task.cqc.graph.name=epinions-txt-prepared
task.cqc.k.value=0

# spark common properties
task.spark.classname=org.SparkCQC.Query3SparkSQL
task.spark.driver.memory=16G
task.spark.executor.memory=360G
task.spark.graph.name=epinions-txt-prepared
task.spark.k.value=0

# postgresql common properties
task.postgresql.query=specs/parallel_processing/q3_epinions/q3_epinions_postgresql.sql

# task specific properties
# parallelism = 1
task1.system.name=io
task1.io.cores.max=1
task1.io.default.parallelism=2
task1.io.executor.cores=1

task2.system.name=cqc
task2.cqc.classname=org.SparkCQC.Query3SparkCQCPlan1
task2.cqc.cores.max=1
task2.cqc.default.parallelism=2
task2.cqc.executor.cores=1

task3.system.name=cqc
task3.cqc.classname=org.SparkCQC.Query3SparkCQCPlan2
task3.cqc.cores.max=1
task3.cqc.default.parallelism=2
task3.cqc.executor.cores=1

task4.system.name=spark
task4.spark.cores.max=1
task4.spark.default.parallelism=2
task4.spark.executor.cores=1

task5.system.name=postgresql
task5.postgresql.parallelism=1

# parallelism = 2
task6.system.name=io
task6.io.cores.max=2
task6.io.default.parallelism=4
task6.io.executor.cores=2

task7.system.name=cqc
task7.cqc.classname=org.SparkCQC.Query3SparkCQCPlan1
task7.cqc.cores.max=2
task7.cqc.default.parallelism=4
task7.cqc.executor.cores=2

task8.system.name=cqc
task8.cqc.classname=org.SparkCQC.Query3SparkCQCPlan2
task8.cqc.cores.max=2
task8.cqc.default.parallelism=4
task8.cqc.executor.cores=2

task9.system.name=spark
task9.spark.cores.max=2
task9.spark.default.parallelism=4
task9.spark.executor.cores=2

task10.system.name=postgresql
task10.postgresql.parallelism=2

# parallelism = 4
task11.system.name=io
task11.io.cores.max=4
task11.io.default.parallelism=8
task11.io.executor.cores=4

task12.system.name=cqc
task12.cqc.classname=org.SparkCQC.Query3SparkCQCPlan1
task12.cqc.cores.max=4
task12.cqc.default.parallelism=8
task12.cqc.executor.cores=4

task13.system.name=cqc
task13.cqc.classname=org.SparkCQC.Query3SparkCQCPlan2
task13.cqc.cores.max=4
task13.cqc.default.parallelism=8
task13.cqc.executor.cores=4

task14.system.name=spark
task14.spark.cores.max=4
task14.spark.default.parallelism=8
task14.spark.executor.cores=4

task15.system.name=postgresql
task15.postgresql.parallelism=4

# parallelism = 8
task16.system.name=io
task16.io.cores.max=8
task16.io.default.parallelism=16
task16.io.executor.cores=8

task17.system.name=cqc
task17.cqc.classname=org.SparkCQC.Query3SparkCQCPlan1
task17.cqc.cores.max=8
task17.cqc.default.parallelism=16
task17.cqc.executor.cores=8

task18.system.name=cqc
task18.cqc.classname=org.SparkCQC.Query3SparkCQCPlan2
task18.cqc.cores.max=8
task18.cqc.default.parallelism=16
task18.cqc.executor.cores=8

task19.system.name=spark
task19.spark.cores.max=8
task19.spark.default.parallelism=16
task19.spark.executor.cores=8

task20.system.name=postgresql
task20.postgresql.parallelism=8

# parallelism = 16
task21.system.name=io
task21.io.cores.max=16
task21.io.default.parallelism=32
task21.io.executor.cores=16

task22.system.name=cqc
task22.cqc.classname=org.SparkCQC.Query3SparkCQCPlan1
task22.cqc.cores.max=16
task22.cqc.default.parallelism=32
task22.cqc.executor.cores=16

task23.system.name=cqc
task23.cqc.classname=org.SparkCQC.Query3SparkCQCPlan2
task23.cqc.cores.max=16
task23.cqc.default.parallelism=32
task23.cqc.executor.cores=16

task24.system.name=spark
task24.spark.cores.max=16
task24.spark.default.parallelism=32
task24.spark.executor.cores=16

task25.system.name=postgresql
task25.postgresql.parallelism=16