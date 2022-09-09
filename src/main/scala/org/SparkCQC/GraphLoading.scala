package org.SparkCQC

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a test program for testing the graph loading performance
 * for setting the lower bound of the running times.
 */
object GraphLoading {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("GraphLoading")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    assert(args.length == 1)
    val path = args(0)
    println(s"load graph file:${path}")
    val graph = sc.textFile(s"file:${path}")
    // first count to force spark to actually load data from disk
    graph.count()

    spark.time(graph.count())
    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark.close()
  }
}
