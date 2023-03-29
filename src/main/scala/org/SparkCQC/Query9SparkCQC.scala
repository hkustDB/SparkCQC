package org.SparkCQC

import org.SparkCQC.ComparisonJoins._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a test program for testing the following SQL query over a graph
 * select g2.src, g2.dst, g3.dst, sum(c1.cnt)
 * from Graph g1, Graph g2, Graph g3,
 * (select src, count(*) as cnt from Graph group by src) as c1,
 * (select src, count(*) as cnt from Graph group by src) as c2
 * where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
 * and g3.dst = c2.src and c1.cnt < c2.cnt
 */
object Query9SparkCQC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Query9SparkCQC")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val defaultParallelism = sc.defaultParallelism

    assert(args.length == 5)
    val path = args(0)
    val file = args(1)
    val k = args(2).toInt
    val ioType = args(3)
    val saveAsTextFilePath = args(4)

    // Modify to the correct input file path
    val lines = sc.textFile(s"file:${path}/${file}")
    val graph1 = lines.map(line => {
      val temp = line.split("\\s+")
      (temp(0).toInt, (temp(0).toInt, temp(1).toInt))
    })
    val graph2 = lines.map(line => {
      val temp = line.split("\\s+")
      (temp(1).toInt, (temp(0).toInt, temp(1).toInt))
    })
    graph1.cache()
    val g2 = graph1.mapValues(x => Array[Any](x._1, x._2))
    val frequency = graph1.map(edge => (edge._1, 1)).reduceByKey((a, b) => a+b)
    // g1 Schema (g1.DST, (g1.SRC, g1.DST, c1.CNT))
    val g1 = graph1.join(frequency).map(x => (x._2._1._2, Array[Any](x._2._1._1, x._2._1._2, x._2._2))).cache()
    // g3 Schema (g3.SRC, (g3.SRC, g3.DST, c2.CNT))
    val g3 = graph2.join(frequency).map(x => (x._2._1._1, Array[Any](x._2._1._1, x._2._1._2, x._2._2))).cache()
    spark.time(println(g1.count()))
    spark.time(println(g2.count()))
    spark.time(println(g3.count()))

    def smaller(x : Int, y : Int) : Boolean = {
      if (x < y) true
      else false
    }

    val C = new ComparisonJoins()
    // g1CoGroup Schema (g1.DST, Array(g1.SRC, g1.DST, c1.CNT)) SortBy c1.CNT increasing
    val g1CoGroup = g1.groupByKey.mapValues(x => x.toArray.sortWith((x, y) => smaller(x.last.asInstanceOf[Int], y.last.asInstanceOf[Int])))
    // g1Annotation Schema (g1.DST, Array(c1.CNT, Annotation)) SortBy c1.CNT increasing
    val g1Annotation = g1CoGroup.mapValues(x => {
      var t : Int = 0
      val result = x.map(y => {
        t = t + y(2).asInstanceOf[Int]
        Array[Any](y(2), t)
      })
      result
    })
    // g1Max Schema (g1.DST, c1.CNT)
    val g1Max = C semijoinSortToMax(g1CoGroup)
    //g2CoGroup Schema g2.DST, Array(g2.SRC, g2.DST, c1.CNT)
    val g2CoGroup = C semijoin(g2, g1Max,  0, 1, smaller(_,_))
    g2CoGroup.cache()
    // g2Max Schema (g2.DST, c1.CNT)
    val g2Max = C semijoinSortToMax(g2CoGroup)
    // enum1 Schema g3.SRC, (g3.SRC, g3.DST, C2.CNT)
    val enum1 = C enumeration1 (g2Max, g3, Array(), Array(0, 1, 2), (1, 0), (2, 2), 0, smaller)
    enum1.cache()
    // enum2 Schema (g2.SRC, (g3.SRC, g3.DST, c2.CNT, g2.SRC))
    val enum2 = C enumeration (enum1, g2CoGroup, Array(0, 1, 2), Array(0), (2, 2), (1, 2), 3, (x: Int, y: Int) => smaller(x, y))
    enum2.cache()
    // enum3 Schema (g3.DST, g3.SRC, g2.SRC, annotation)
    //val enum3 = C enumeration (enum2, g1CoGroup, Array(0, 1, 2, 3), Array(0, 2), (2, 2), (1, 1), 4, (x: Int, y: Int) => smaller(x + k, y))
    val enum3 = C enumerationWithAnnotation (enum2, g1Annotation, Array(0, 1, 3), Array(1),  2, 0, 0, smaller)
    if (ioType != "no_io")
      IOTaskHelper.saveResultAsTextFile(enum3, ioType, saveAsTextFilePath, defaultParallelism)
    else
      spark.time(print(enum3.count()))
    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark.close()
  }
}
