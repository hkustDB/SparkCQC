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
object Query10TBSparkCQC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Query10TBSparkCQC")

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
    // val g2 = graph1.mapValues(x => Array[Any](x._1, x._2))
    val frequency = graph1.map(edge => (edge._1, 1)).reduceByKey((a, b) => a+b)
    // g1 Schema (g1.DST, (g1.SRC, g1.DST, c1.CNT))
    val g1 = graph1.join(frequency).map(x => (x._2._1._2.asInstanceOf[Any], Array[Any](x._2._1._1, x._2._1._2, x._2._2))).cache()
    // g2 Schema (g2.DST, (g2.SRC, g2.DST, count(g2))
    val g2 = g1.cache()
    // g3 Schema (g3.SRC, (g3.SRC, g3.DST, c2.CNT))
    val g3 = graph2.join(frequency).map(x => (x._2._1._1.asInstanceOf[Any], Array[Any](x._2._1._1, x._2._1._2, x._2._2))).cache()
    val n = g2.count()

    spark.time(println(g1.count()))
    spark.time(println(g2.count()))
    spark.time(println(g3.count()))

    // g2l/g2r Schema (g2.DST, (g2.SRC, g2.DST, count(g2))
    val g2l = g2.filter(x => x._2(2).asInstanceOf[Int] <= Math.sqrt(n.toDouble))
    val g2r = g2.filter(x => x._2.asInstanceOf[Int] > Math.sqrt(n.toDouble))

    // bag1 Schema (g2.DST, (g1.SRC, g1.DST, g2.DST, annotation, c1.CNT))
    val bag1 = g1.join(g2l).map(x => (x._2._2(1), Array[Any](x._2._1(0), x._2._1(1),  x._2._2(1), 1.asInstanceOf[Int], x._2._1(2))))
    //bag2 Schema (g2.SRC, (g2.SRC, g2.DST, g3.DST, c2.CNT))
    val bag2 = g2r.join(g3).map(x => (x._2._1(0), Array[Any](x._2._1(0), x._2._1(1), x._2._2(1), x._2._2(2))))
    def smaller(x : Int, y : Int) : Boolean = {
      if (x < y) true
      else false
    }

    val C = new ComparisonJoins()
    // bag1CoGroup Schema (g2.DST, Array(g1.SRC, g1.DST, g2.DST, annotation, c1.CNT)) SortBy c1.CNT increasing
    val bag1CoGroup = bag1.groupByKey.mapValues(x => x.toArray.sortWith((x, y) => smaller(x.last.asInstanceOf[Int], y.last.asInstanceOf[Int])))
    // bag1Annotation Schema (g2.DST, Array(c1.CNT, Annotation)) SortBy c1.CNT increasing
    val bag1Annotation = bag1CoGroup.mapValues(x => {
      var t : Int = 0
      val result = x.map(y => {
        t = t + y(3).asInstanceOf[Int]
        Array[Any](y(4), t)
      })
      result
    })
    // bag1Max Schema (g2.DST, c1.CNT)
    val bag1Max = C semijoinSortToMax(bag1CoGroup)
    // enum1 Schema g3.SRC, (g3.SRC, g3.DST, C2.CNT)
    val enum1 = C enumeration1 (bag1Max, g3, Array(), Array(0, 1, 2), (1, 0), (2, 2), 0, smaller)
    enum1.cache()
    // enum3 Schema (g3.SRC, g3.DST, annotation)
    val enum3 = C enumerationWithAnnotation (enum1, bag1Annotation, Array(0, 1), Array(1),  2, 0, 0, smaller)
    val result1=enum3.map(x=> ((x._2(0).asInstanceOf[Int], x._2(1).asInstanceOf[Int]), x._2(2).asInstanceOf[Int]))

    // g1CoGroup Schema (g1.DST, Array(g1.SRC, g1.DST, c1.CNT)) Sort By c1.CNT increasing
    val g1CoGroup = g1.groupByKey.mapValues(x => x.toArray.sortWith((x, y) => smaller(x.last.asInstanceOf[Int], y.last.asInstanceOf[Int])))
    // g1Annotation Schema (g1.DST, Array(c1.CNT, annotation)) SOrt By c1.CNT increasing
    val g1Annotation = g1CoGroup.mapValues(x => {
      var t: Int = 0
      val result = x.map(y => {
        t = t + 1
        Array[Any](y(4), t)
      })
      result
    })

    // g1Max Schema (g1.DST, c1.CNT)
    val g1Max = C semijoinSortToMax (g1CoGroup)
    // enum4 Schema g2.SRC, Array(g2.SRC, g3.SRC, g3.DST, C2.CNT)
    val enum4 = C enumeration1 (g1Max, bag2, Array(), Array(0, 1, 2, 3), (1, 0), (2, 3), 0, smaller)
    // enum5 Schema g2.SRC, (g2.SRC, g3.SRC, g3.DST, annotation)
    val enum5 = C enumerationWithAnnotation (enum4, g1Annotation, Array(0, 1, 2), Array(1), 2, 0, 0, smaller)
    // result2 Schema (g3.SRC, g3.DST), annotation
    val result2=enum5.map(x=> ((x._2(0).asInstanceOf[Int], x._2(1).asInstanceOf[Int]), x._2(2).asInstanceOf[Int])).reduceByKey((x, y) => x+y)

    val result = result1.join(result2).map(x => x._2._1 + x._2._2)

    if (ioType != "no_io")
      IOTaskHelper.saveResultAsTextFile(result, ioType, saveAsTextFilePath, defaultParallelism)
    else
      spark.time(print(result.count()))
    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark.close()
  }
}
