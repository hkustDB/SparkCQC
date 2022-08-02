package org.SparkCQC

import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.SparkCQC.ComparisonJoins._

/**
 * This is a test program for testing the following SQL query over a graph
 * SELECT g1.src, g2.src, g3.src, g4.src, g5.src, g6.src
 * From Graph g1, Graph g2, Graph g3, Graph g4, Graph g5, Graph g6, Graph g7
 * where g1.src = g3.dst and g2.src = g1.dst and g3.src=g2.dst
 * and g4.src = g6.dst and g5.src = g4.dst and g6.src = g5.dst
 * and g1.dst = g7.src and g4.src = g7.dst and
 * g1.weight*g2.weight*g3.weight < g4.weight*g5.weight*g6.weight
 */
object Query2SparkCQC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Query2SparkCQC")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    sc.defaultParallelism

    assert(args.length == 3)
    val path = args(0)
    val file = args(1)
    val saveAsTextFilePath = args(2)

    // Modify to the correct input file path
    val lines = sc.textFile(s"${path}/${file}")

    def smaller(x: Int, y: Int): Boolean = {

      if (x < y) true
      else false
    }

    val C = new ComparisonJoins()
    val graph = lines.map(line => {
      val temp = line.split(",")
      (temp(0).toInt, Array[Any](temp(0).toInt, temp(1).toInt, temp(2).toInt))
    })
    graph.cache()
    val g1 = graph.cache()
    val g1revise = g1.map(x => (x._2(1).asInstanceOf[Int], x._2))
    g1revise.cache()
    val g2 = g1.map(x => ((x._2(1), x._2(0)), x._2))
    val L2 = g1revise.join(g1).map(x => {
      ((x._2._1(0), x._2._2(1)), Array(x._2._1(0), x._2._1(1), x._2._2(1), x._2._1(2), x._2._2(2)))
    })
    val g0 = graph.map(x => (x._1, Array(x._2(0), x._2(1)))).cache()
    // Schema A, B, C, Prod(Weight)
    val triangle = L2.join(g2).map(x => {
      (x._1, Array[Any](x._2._1(0), x._2._1(1), x._2._1(2),
        x._2._1(3).asInstanceOf[Int] * x._2._1(4).asInstanceOf[Int] * x._2._2(2).asInstanceOf[Int]))
    })
    // Schema A, B, C, Prod(Weight) Key B
    val triangleB = triangle.map(x => (x._2(1).asInstanceOf[Int], x._2)).partitionBy(new HashPartitioner(2)).cache()
    // Schema B, Array(A, B, C, Prod(Weight))
    val triangleBGroup = C groupBy(triangleB, 3, smaller)
    triangleBGroup.cache()
    // Schema B, Min(Weight)
    val triangleBMax = C semijoinSortToMax triangleBGroup
    // Schema A, B, C, Prod(Weight) Key A
    val triangleA = triangle.map(x => (x._2(0).asInstanceOf[Int], x._2)).cache()

    val triangleG1 = C.semijoin(g0, triangleBMax, 0, 1, smaller(_, _))
    triangleG1.cache()
    val triangleG1Max = C.semijoinSortToMax(triangleG1)
    // Schema B, Prod(Weight)
    val cnt = C.enumeration1(triangleG1Max, triangleA, Array(), Array(0, 1, 2, 3), (1, 0), (2, 3), 0, smaller)
    spark.time(println(cnt.count()))
    val enum1 = C.enumeration(cnt, triangleG1, Array(0, 1, 2, 3), Array(0), (2, 2), (1, 3), 4, smaller(_, _))
    spark.time(println(enum1.count()))

    val enum2 = C.enumeration(enum1, triangleBGroup, Array(0, 1, 2, 3), Array(0, 1, 2, 3), (2, 3), (1, 3), 0, smaller(_, _))

    if (saveAsTextFilePath.nonEmpty)
      enum2.saveAsTextFile(saveAsTextFilePath)
    else
      spark.time(println(enum2.count()))

    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark.close()
  }
}
