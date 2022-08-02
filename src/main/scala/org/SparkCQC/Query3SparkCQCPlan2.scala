package org.SparkCQC

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.SparkCQC.ComparisonJoins._

import scala.collection.mutable.ListBuffer

/**
 * This is a test program for testing the following SQL query over a graph
 * select g1.src, g1.dst, g2.dst, g3.dst, c1.cnt, c2.cnt, c3.cnt, c4.cnt
 * from Graph g1, Graph g2, Graph g3,
 * (select src, count(*) as cnt from Graph group by src) as c1,
 * (select src, count(*) as cnt from Graph group by src) as c2,
 * (select src, count(*) as cnt from Graph group by src) as c3
 * (select dst, count(*) as cnt from Graph group by dst) as c4,
 * where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
 * and g3.dst = c2.src and g3.dst = c4.dst and g2.src = c3.src
 * and c1.cnt < c2.cnt and c3.cnt < c4.cnt
 */
object Query3SparkCQCPlan2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Query3SparkCQCPlan2")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    sc.defaultParallelism

    assert(args.length == 3)
    val path = args(0)
    val file = args(1)

    val lines = sc.textFile(s"${path}/${file}")

    val graph = lines.map(line => {
      val temp = line.split("\\s+")
      (temp(0).toInt, Array[Any](temp(0).toInt, temp(1).toInt))
    })
    val graphinvert = graph.map(x => (x._2(1).asInstanceOf[Int], x._2)).cache()
    spark.time(println(graphinvert.count()))
    graph.cache()
    spark.time(println(graph.count()))
    val frequency = graph.map(edge => (edge._1, 1)).reduceByKey((a, b) => a + b).cache()
    val frequency0 = graph.map(edge => (edge._2(1).asInstanceOf[Int], 1)).reduceByKey((a, b) => a + b).cache()

    val g1 = graph.join(frequency).map(x => (x._1, Array[Any](x._2._1(0), x._2._1(1), x._2._2))).cache()

    // g2 Schema (g3.src, g3.dst, c4.cnt, c2.cnt)
    val g2 = graphinvert.join(frequency0)
        .map(x => (x._1.asInstanceOf[Int], Array[Any](x._2._1(0), x._2._1(1), x._2._2))).join(frequency)
        .map(x => (x._2._1(0).asInstanceOf[Int], Array[Any](x._2._1(0), x._2._1(1), x._2._1(2), x._2._2))).cache()


    def smaller(x: Int, y: Int): Boolean = {

      if (x < y) true
      else false
    }

    def larger(x: Int, y: Int): Boolean = {
      if (x > y) true
      else false
    }

    val C = new ComparisonJoins()
    // g3CoGroup Schema (g3.SRC, g3.DST, c4.CNT, c2.CNT)
    val g3CoGroup = C groupBy(g2, 2, 3, larger(_, _), larger(_, _))
    g3CoGroup.cache()
    //g3Max Schema (C4.CNT, C2.CNT)
    val g3Max = g3CoGroup.mapValues(x => x.toSmall)
    // g2CoGroup Schema (g2.SRC, g2.DST, c3.CNT, c2.CNT)
    val g1reverse = g1.map(x => (x._2(1).asInstanceOf[Int], x._2)).cache()

    val g2CoGroup = C semijoin(g1reverse, g3Max, 2, larger(_, _), 0)
    g2CoGroup.cache()
    val g2Max = C semijoinSortToMax (g2CoGroup)
    val g1semiJoin = g1reverse.cogroup(g2Max).flatMap(x => {
      val result = ListBuffer[(Int, Array[Any])]()
      for (i <- x._2._1) {
        if (x._2._2.size > 1) throw new Exception("Not unique!")
        for (j <- x._2._2) {
          if (larger(j(0).asInstanceOf[Int], i(2).asInstanceOf[Int])) {
            result.append((x._1, i))
          }
        }
      }
      result
    })

    // cnt Schema (g1.SRC, g1.DST, c1.CNT, g2.SRC, g2.DST, c3.CNT)
    val cnt = C enumeration(g1semiJoin, g2CoGroup, Array(0, 1, 2), Array(0, 1, 2), (2, 3), (1, 2), 4, larger(_, _))
    // cntmap Schema (g1.SRC, g1.DST, c3.CNT, c1.CNT, g2.SRC, g2.DST)
    val cntmap = cnt.mapValues(x => {
      val tmp = x(2)
      x(2) = x(5)
      x(5) = x(4)
      x(4) = x(3)
      x(3) = tmp
      x
    })

    // enum1 Schema (g1.SRC, c3.cnt, c1.cnt, g2.src, g2.dst, g3.dst, c3.cnt, c2.cnt)
    val enum1 = C enumeration(cntmap, g3CoGroup, Array(0, 2, 3, 4, 5), Array(1, 2, 3), 2, 3, 6)

    spark.time(println(enum1.count()))

    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark.close()
  }
}
