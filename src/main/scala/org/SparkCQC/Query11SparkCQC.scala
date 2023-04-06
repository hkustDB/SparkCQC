package org.SparkCQC

import org.SparkCQC.ComparisonJoins._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * This is a test program for testing the following SQL query
 * SELECT T1.CA_ID, T1.S_SYMB, MAX(T2.T_TRADE_PRICE-T1.T_TRADE_PRICE)
 * FROM Trade T1, Trade T2
 * WHERE T1.TT = "BUY" AND T2.TT = "SALE" AND T1.CA_ID = T2.CA_ID AND T1.S_SYBM = T2.S_SYMB
 * AND T1.T_DTS <= T2.T_DTS AND T1.T_DTS + interval '90' day >= T2.T_DTS
 * GROUP BY T1.CA_ID, T1.S_SYBM
 */
object Query11SparkCQC {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("Query11SparkCQC")
        val sc = new SparkContext(conf)

        val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

        assert(args.length >= 2)
        val path = args(0)
        val file = args(1)

        val dbs = sc.textFile(s"${path}/tradeS.txt").coalesce(32).map(line => {
            val temp = line.split("\\|")
            ((temp(2), temp(3).toLong), Array(temp(0).toLong, temp(1).toLong, temp(2), temp(3).toLong, temp(4).toDouble, temp(1).toLong))
        }).partitionBy(new HashPartitioner(32)).cache()

        val dbb = sc.textFile(s"${path}/tradeB.txt").coalesce(32).map(line => {
            val temp = line.split("\\|")
            ((temp(2), temp(3).toLong), Array(temp(0).toLong, temp(1).toLong, temp(2), temp(3).toLong, temp(4).toDouble, temp(1).toLong+7776000000L))
        }).partitionBy(new HashPartitioner(32)).cache()

        spark.time(println(dbb.count()))
        spark.time(println(dbs.count()))

        def smallerL(x: Long, y: Long): Boolean = {
            if (x < y) true
            else false
        }

        def largerL(x: Long, y: Long): Boolean = {
            if (x >= y) true
            else false
        }

        def sumAnnotation(x : Double, y : Double) : Double = {
            if (x > y) x else y
        }

        def productAnnotation(x : Double, y : Double) : Double = {
            x * y
        }

        val C = new ComparisonJoins()

        val dbbGroup = C.groupBy(dbb, 1, 5, smallerL, largerL, 4, sumAnnotation, -1000000.00).cache()
        val dbbMax = dbbGroup.mapValues(x => x.toSmall).cache()
        val dbsemiJoin = C.semijoin(dbbMax, dbs, 1, 5, smallerL, largerL).cache()
        val result = dbsemiJoin.cogroup(dbbGroup).flatMap(
            x => for {
                y <- x._2._1
                result = productAnnotation(y(4).asInstanceOf[Double],
                    x._2._2.head.findAnnotation(y(1).asInstanceOf[Long], y(5).asInstanceOf[Long]))

            } yield (x._1, result))

        val result2 = result.reduceByKey((x, y) => sumAnnotation(x, y))
        spark.time(print(result2.count()))

        println("APP Name :" + spark.sparkContext.appName)
        println("Deploy Mode :" + spark.sparkContext.deployMode)
        println("Master :" + spark.sparkContext.master)

        spark.close()
    }
}
