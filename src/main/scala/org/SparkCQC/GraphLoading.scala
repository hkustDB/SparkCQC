package org.SparkCQC

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This is a test program for testing the graph loading performance
 * for setting the lower bound of the running times.
 */
object GraphLoading extends App {
  val conf = new SparkConf()
  conf.setAppName("GraphLoading")

  val sc = new SparkContext(conf)

  val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

  // Set the file path to the test file.
  val graph = sc.textFile("Temporal.csv")

  spark.time(graph.count())
  println("First SparkContext:")
  println("APP Name :" + spark.sparkContext.appName)
  println("Deploy Mode :" + spark.sparkContext.deployMode)
  println("Master :" + spark.sparkContext.master)

  spark.close()
}
