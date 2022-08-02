package org.SparkCQC

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

/**
 * list all the files that end with ".csv" or ".txt" under the given path and rewrite them as {name}-prepared
 * with minPartitions = 32.
 */
object GraphPreparation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("GraphPreparation")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    assert(args.length == 1)
    val path = args(0)
    println(s"prepare for graphs in ${path}")
    listGraphFiles(args(0)).foreach(name => {
      val graph = sc.textFile(s"${path}/${name}", 32)
      val targetName = s"${name.replace(".", "-")}-prepared"
      println(s"write ${name} as ${targetName}")
      graph.saveAsTextFile(s"${path}/${targetName}")
    })

    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)
    spark.close()
  }

  def listGraphFiles(dir: String): List[String] = {
    val directory = new File(dir)
    assert(directory.exists() && directory.isDirectory)
    directory.listFiles(f => f.isFile)
        .map(f => f.getName)
        .filter(t => t.endsWith(".csv") || t.endsWith(".txt"))
        .toList
  }
}
