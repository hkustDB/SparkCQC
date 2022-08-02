package org.SparkCQC

import org.SparkCQC.ComparisonJoins.ComparisonJoins
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a test program for testing the following SQL query over a graph
 * select g2.src, g2.dst
 * from Graph g1, Graph g2, Graph g3, Graph g4, Graph g5,
 * (select src, count(*) as cnt from Graph group by src) as c1,
 * (select src, count(*) as cnt from Graph group by src) as c2,
 * (select dst, count(*) as cnt from Graph group by dst) as c3,
 * (select dst, count(*) as cnt from Graph group by dst) as c4
 * where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
 * and g3.dst = c2.src and c1.cnt < c2.cnt
 * and g4.dst = g2.src and g2.dst = g5.src and g4.src = c3.dst
 * and g5.dst = c4.dst and c3.cnt < c4.cnt
 */
object Query5SparkCQC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Query5SparkCQC")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    assert(args.length == 3)
    val path = args(0)
    val file = args(1)
    val saveAsTextFilePath = args(2)

    val lines = sc.textFile(s"${path}/${file}")
    val graph = lines.map(line => {
      val temp = line.split("\\s+")
      (temp(0).toInt, temp(1).toInt)
    })
    val reversedGraph = graph.map(x => (x._2, x._1))
    graph.cache()
    graph.count()
    val frequency = graph.map(edge => (edge._1, 1)).reduceByKey((a, b) => a + b).cache()
    frequency.count()
    val frequency2 = graph.map(edge => (edge._2, 1)).reduceByKey((a, b) => a + b).cache()
    frequency2.count()
    val g = graph.map(x => (x._1, Array[Any](x._1, x._2))).cache()
    val g1 = graph.join(frequency).map(x => (x._1, x._2._1, x._2._2)).map(x => (x._2, Array[Any](x._1, x._2, x._3))).cache()
    val g3 = graph.join(frequency2).map(x => (x._1, x._2._1, x._2._2)).map(x => (x._2, Array[Any](x._1, x._2, x._3))).cache()
    val g2 = reversedGraph.join(frequency).map(x => (x._2._1, x._1, x._2._2)).map(x => (x._1, Array[Any](x._1, x._2, x._3))).cache()
    val g4 = reversedGraph.join(frequency2).map(x => (x._2._1, x._1, x._2._2)).map(x => (x._1, Array[Any](x._1, x._2, x._3))).cache()
    g1.count()
    g2.count()
    g3.count()
    g4.count()

    def smaller(x: Int, y: Int): Boolean = {
      if (x < y) true
      else false
    }

    def larger(x: Int, y: Int): Boolean = {
      if (x > y) true
      else false
    }

    val C = new ComparisonJoins()
    // g1CoGroup Schema (g1.SRC, g1.DST, c1.CNT)
    val g1Max = g1.reduceByKey((x, y) => if (x(2).asInstanceOf[Int] < y(2).asInstanceOf[Int]) x else y)
    //val g1Max = C.getMinimal(g1, 2, smaller)
    val g2Max = g2.reduceByKey((x, y) => if (x(2).asInstanceOf[Int] > y(2).asInstanceOf[Int]) x else y)
    val g3Max = g3.reduceByKey((x, y) => if (x(2).asInstanceOf[Int] < y(2).asInstanceOf[Int]) x else y)
    val g4Max = g4.reduceByKey((x, y) => if (x(2).asInstanceOf[Int] > y(2).asInstanceOf[Int]) x else y)
    val semiJoin1 = g.join(g1Max).map(x => (x._2._1(1).asInstanceOf[Int], (x._2._1, x._2._2(2))))
    val semiJoin2 = semiJoin1.join(g2Max).map(x =>
      (x._2._1._1(0), (x._2._1._1, x._2._1._2, x._2._2(2)))
    ).filter(x => smaller(x._2._2.asInstanceOf[Int], x._2._3.asInstanceOf[Int]))
        .map(x => (x._1.asInstanceOf[Int], x._2._1))
    val semiJoin3 = semiJoin2.join(g3Max).map(x => (x._2._1(1).asInstanceOf[Int], (x._2._1, x._2._2(2))))
    val result = semiJoin3.join(g4Max).map(x =>
      (x._2._1._1(0), (x._2._1._1, x._2._1._2, x._2._2(2)))
    ).filter(x => smaller(x._2._2.asInstanceOf[Int], x._2._3.asInstanceOf[Int]))
        .map(x => (x._1.asInstanceOf[Int], x._2._1))

    if (saveAsTextFilePath.nonEmpty)
      result.saveAsTextFile(saveAsTextFilePath)
    else
      spark.time(print(result.count()))

    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark.close()
  }
}
