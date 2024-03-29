package org.SparkCQC

import org.SparkCQC.ComparisonJoins._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.util.Random

/**
 * This is a test program for testing the following SQL query over a graph
 * select g1.src, g1.dst, g2.dst, g3.dst, c1.cnt, c2.cnt, c3.cnt, c4.cnt
 * from Graph g1, Graph g2, Graph g3,
 * (select src, count(*) as cnt from Graph group by src) as c1,
 * (select src, count(*) as cnt from Graph group by src) as c2,
 * (select src, count(*) as cnt from Graph group by src) as c3,
 * (select dst, count(*) as cnt from Graph group by dst) as c4
 * where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
 * and g3.dst = c2.src and g3.dst = c4.dst and g2.src = c3.src
 * and c1.cnt + k < c2.cnt and c3.cnt < c4.cnt
 */
object Query3SparkCQCPlan1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Query3SparkCQCPlan1")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val defaultParallelism = sc.defaultParallelism

    assert(args.length == 5)
    val path = args(0)
    val file = args(1)
    val k = args(2).toInt
    val ioType = args(3)
    val saveAsTextFilePath = args(4)

    val lines = sc.textFile(s"file:${path}/${file}")
    val graph = lines.map(line => {
      val temp = line.split("\\s+")
      (temp(0).toInt, Array[Any](temp(0).toInt, temp(1).toInt))
    })
    val graphinvert = graph.map(x => (x._2(1).asInstanceOf[Int], x._2)).cache()
    spark.time(println(graphinvert.count()))
    graph.cache()
    spark.time(graph.count())
    val frequency = graph.map(edge => (edge._1, 1)).reduceByKey((a, b) => a + b).cache()
    val frequency0 = graph.map(edge => (edge._2(1).asInstanceOf[Int], 1)).reduceByKey((a, b) => a + b).cache()
    val frequencyAddK = frequency.mapValues(t => t + k)

    val g1 = graph.join(frequency).map(x => (x._1, Array[Any](x._2._1(0), x._2._1(1), x._2._2))).cache()
    val g1AddK = graph.join(frequencyAddK).map(x => (x._1, Array[Any](x._2._1(0), x._2._1(1), x._2._2))).cache()
    // g2 Schema (g3.src, g3.dst, c4.cnt, c2.cnt)
    val g2 = graphinvert.join(frequency0)
        .map(x => (x._1.asInstanceOf[Int], Array[Any](x._2._1(0), x._2._1(1), x._2._2))).join(frequency)
        .map(x => (x._2._1(0).asInstanceOf[Int], Array[Any](x._2._1(0), x._2._1(1), x._2._1(2), x._2._2))).cache()

    def smaller(x: Int, y: Int): Boolean = {
      if (x < y) true
      else false
    }

    val C = new ComparisonJoins()
    // g1CoGroup Schema (g1.SRC, g1.DST, c1.CNT)
    val g1CoGroup = C groupBy(g1AddK, 2, smaller, 1)
    //spark.time(println(g1CoGroup.count()))
    g1CoGroup.cache()
    // g1Max Schema (g1.DST, c1.CNT)
    val g1Max = C semijoinSortToMax (g1CoGroup)

    //g2CoGroup Schema (g2.SRC, g2.DST, c3.CNT, c1.CNT)
    val g2CoGroup = C.semijoin(g1, g1Max, 0, 3, 2, smaller, smaller, 1)
    g2CoGroup.cache()

    // g2Max Schema (g2.DST, c1.CNT, c3.CNT)
    val g2Max = g2CoGroup.mapValues(x => x.toSmall)
    // g3semiJoin Schema (g3.src, g3.dst, c4.cnt, c2.cnt)
    val g3semiJoin = C.semijoin(g2Max, g2, 3, 2, smaller(_, _), smaller(_, _))

    // cnt Schema (g3.SRC, g3.DST, c4.cnt, c2.cnt, g2.src, c3.cnt)
    val cnt = C enumeration(g3semiJoin, g2CoGroup, Array(0, 1, 2, 3), Array(0, 2), 3, 2, 4)

    // enum1 Schema (g3.SRC, g3.DST, c4.cnt, c2.cnt, g2.src, c3.cnt, g1.src, c1.cnt)
    val enum1 = C enumeration(cnt, g1CoGroup, Array(0, 1, 2, 3, 4, 5), Array(0, 2), (2, 2), (1, 3), 6, smaller(_, _))

    if (ioType != "no_io")
      IOTaskHelper.saveResultAsTextFile(enum1, ioType, saveAsTextFilePath, defaultParallelism)
    else
      spark.time(print(enum1.count()))



    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark.close()
  }
}
