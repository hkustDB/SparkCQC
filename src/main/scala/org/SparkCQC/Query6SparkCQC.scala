package org.SparkCQC

import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.SparkCQC.ComparisonJoins._

import scala.util.Random

/**
 * This is a test program for testing the following SQL query
 * SELECT * FROM Trade T1, Trade T2
 * WHERE T1.TT = "BUY" and T2.TT = "SALE"
 * and T1.CA_ID = T2.CA_ID
 * and T1.S_SYBM = T2.S_SYMB
 * and T1.T_DTS <= T2.T_DTS
 * and T1.T_DTS + interval '90' day >= T2.T_DTS
 * and T1.T_TRADE_PRICE*1.2 < T2.T_TRADE_PRICE
 */
object Query6SparkCQC {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("Query6SparkCQC")
        val sc = new SparkContext(conf)

        val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

        val defaultParallelism = sc.defaultParallelism

        assert(args.length == 4)
        val path = args(0)
        val file = args(1)
        val saveAsTextFilePath = args(2)

        val partitioner = new HashPartitioner(32)

        val dbs = sc.textFile(s"${path}/tradeS.txt").coalesce(32).map(line => {
            val temp = line.split("\\|")
            ((temp(2), temp(3).toLong), Array(temp(0).toLong, temp(1).toLong, temp(2), temp(3).toLong, temp(4).toDouble))
        }).partitionBy(partitioner).cache()

        val dbb = sc.textFile(s"${path}/tradeB.txt").coalesce(32).map(line => {
            val temp = line.split("\\|")
            ((temp(2), temp(3).toLong), Array(temp(0).toLong, temp(1).toLong, temp(2), temp(3).toLong, temp(4).toDouble))
        }).partitionBy(partitioner).cache()

        spark.time(println(dbb.count()))
        spark.time(println(dbs.count()))

        def largerL(x: Long, y: Long): Boolean = {

            if (x > y) true
            else false
        }

        def smallerL(x: Long, y: Long): Boolean = {

            if (x < y) true
            else false
        }

        def smallerD(x: Double, y: Double): Boolean = {
            if (x < y) true
            else false
        }

        val C = new ComparisonJoins()


        val dbbMax = C.getMinimal(dbb, 1, largerL).cache()

        val dbsSemiJoin = C.enumerationnp(dbbMax, dbs, Array(), Array(0, 1, 2, 3, 4), (1, 0), (2, 1), largerL(_, _)).cache()

        val result = dbb.cogroup(dbsSemiJoin).flatMapValues(x => {
            new Iterator[Array[Any]] {
                private var hasOutput = true
                private var iterA: Iterator[Array[Any]] = _
                private var iterB: Iterator[Array[Any]] = _
                private var i: Array[Any] = _
                private var initial = false
                private var nextOutput: Array[Any] = null

                override def hasNext: Boolean = {
                    if (nextOutput != null) return true
                    if (!initial) {
                        hasOutput = false
                        initial = true
                        iterA = x._1.toIterator
                        while (!hasOutput && (iterA.hasNext || iterB.hasNext)) {
                            i = iterA.next
                            iterB = x._2.toIterator
                            while (!hasOutput && iterB.hasNext) {
                                val j = iterB.next
                                if (largerL(i(1).asInstanceOf[Long], j(1).asInstanceOf[Long]))
                                    if (smallerD(i(4).asInstanceOf[Double], j(4).asInstanceOf[Double]) && smallerL(i(1).asInstanceOf[Long] - 7776000000L, j(1).asInstanceOf[Long])) {
                                        nextOutput = Array(i(0), i(2), i(3), j(0), j(2), j(3))
                                        hasOutput = true
                                        return true
                                    }
                            }
                        }
                    }
                    if (hasOutput) {
                        hasOutput = false
                        while (!hasOutput && (iterA.hasNext || iterB.hasNext)) {
                            if (!iterB.hasNext) {
                                iterB = x._2.toIterator
                                i = iterA.next()
                            }
                            while (!hasOutput && iterB.hasNext) {
                                val j = iterB.next
                                if (largerL(i(1).asInstanceOf[Long], j(1).asInstanceOf[Long]))
                                    if (smallerD(i(4).asInstanceOf[Double], j(4).asInstanceOf[Double]) && smallerL(i(1).asInstanceOf[Long] - 7776000000L, j(1).asInstanceOf[Long])) {
                                        nextOutput = Array(i(0), i(2), i(3), j(0), j(2), j(3))
                                        hasOutput = true
                                        return true
                                    }
                            }
                        }
                        hasOutput
                    } else false
                }

                override def next(): Array[Any] = {
                    if (!initial) {
                        if (this.hasNext) {
                            val t = nextOutput
                            nextOutput = null
                            t
                        } else {
                            null
                        }
                    } else {
                        val t = nextOutput
                        nextOutput = null
                        t
                    }
                }
            }
        })

        if (saveAsTextFilePath.nonEmpty) {
            val result2 = result.partitionBy(new Partitioner {
                override def numPartitions: Int = defaultParallelism
                override def getPartition(key: Any): Int = Random.nextInt(defaultParallelism)
            })
            result2.saveAsTextFile(saveAsTextFilePath)
        } else
            spark.time(print(result.count()))

        println("APP Name :" + spark.sparkContext.appName)
        println("Deploy Mode :" + spark.sparkContext.deployMode)
        println("Master :" + spark.sparkContext.master)

        spark.close()
    }
}
