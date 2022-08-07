package org.SparkCQC

import org.SparkCQC.ComparisonJoins.ComparisonJoins
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a test program for testing the following SQL query over a graph
 * select g3.src, g3.dst
 * from Graph g1, Graph g2, Graph g3,
 * (select src, count(*) as cnt from Graph group by src) as c1,
 * (select src, count(*) as cnt from Graph group by src) as c2
 * where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
 * and g3.dst = c2.src and c1.cnt < c2.cnt
 */
object Query4SparkCQC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Query4SparkCQC")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    sc.defaultParallelism
    assert(args.length == 4)
    val path = args(0)
    val file = args(1)
    val saveAsTextFilePath = args(2)

    val lines = sc.textFile(s"${path}/${file}")
    val graph = lines.map(line => {
      val temp = line.split("\\s+")
      (temp(0).toInt, Array[Any](temp(0).toInt, temp(1).toInt))
    })
    graph.cache()
    val frequency = graph.map(edge => (edge._1, 1)).reduceByKey((a, b) => a + b)
    val frequency1 = frequency.map(x => (x._1, Array[Any](x._1, x._2))).cache()
    val frequency2 = frequency.map(x => (x._1, Array[Any](x._1, x._2))).cache()
    val g1 = graph.cache()
    spark.time(println(g1.count()))
    spark.time(println(frequency1.count()))
    spark.time(println(frequency2.count()))


    def smaller(x: Int, y: Int): Boolean = {

      if (x < y) true
      else false
    }

    val C = new ComparisonJoins()
    // g1CoGroup Schema (g1.SRC, g1.DST, c1.CNT)
    val g1CoGroup = C semijoin(g1, frequency1, 1, 1, smaller(_, _))
    g1CoGroup.cache()
    // g1Max Schema (g1.DST, c1.CNT)
    val g1Max = C semijoinSortToMax (g1CoGroup)
    //g2CoGroup Schema (g2.SRC, g2.DST, c1.CNT)
    val g2CoGroup = C semijoin(g1, g1Max, 0, 1, smaller(_, _))
    g2CoGroup.cache()
    // g2Max Schema (g2.DST, c1.CNT)
    val g2Max = C semijoinSortToMax (g2CoGroup)
    // g3CoGroup Schema (g3.SRC, g3.DST, c1.CNT)
    val g3CoGroup = C semijoin(g1, g2Max, 0, 1, smaller(_, _))
    g3CoGroup.cache()
    // g3Max Schema (g3.DST, c1.CNT)
    val g3Max = C semijoinSortToMax (g3CoGroup)
    // cnt Schema (g3.DST, c2.CNT)
    val cnt = C enumeration1(g3Max, frequency2, Array(), Array(0, 1), (1, 0), (2, 1), 0, smaller)
    // enum1 Schema (g3.DST, c2.CNT, g3.SRC)
    val enum1 = C enumeration(cnt, g3CoGroup, Array(0, 1), Array(0), (2, 2), (1, 1), 2, smaller(_, _))
    enum1.cache()

    if (saveAsTextFilePath.nonEmpty)
      enum1.saveAsTextFile(saveAsTextFilePath)
    else
      spark.time(print(enum1.count()))

    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark.close()
  }
}
