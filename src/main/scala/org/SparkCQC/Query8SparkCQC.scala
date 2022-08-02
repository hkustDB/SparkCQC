package org.SparkCQC

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.SparkCQC.ComparisonJoins.ComparisonJoins

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

/**
 * This is a test program for testing the following SQL query
 * SELECT H1.CK, H2.CK, COUNT(DISTINCT H1.SK)
 * FROM Hold H1, Hold H2
 * WHERE H1.SK = H2. SK and H1.CK <> H2.CK
 * and H1.ST < H2.ET - interval '10' day
 * and H2.ST < H1.ET - interval '10' day
 * GROUP BY H1.CK, H2.CK
 */
object Query8SparkCQC extends App {


  val conf = new SparkConf()
  conf.setAppName("Query8SparkCQC")
  val sc = new SparkContext(conf)

  val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

  val lines = sc.textFile("Holding.txt",1).coalesce(1)

  val db = lines.map(line => {
    val temp = line.split("\\|")
    (temp(0).toLong, temp(1), format.parse(temp(2)).getTime, format.parse(temp(3)).getTime)
  }).coalesce(32)
  db.cache()
  val difference = 864000000L
  val h1 = db.map(x => ((x._1, x._2), Array[Any](x._1, x._2, x._3, -x._4))).cache()
  val h2 = db.map(x => ((x._1, x._2), Array[Any](x._1, x._2, x._4-difference,-x._3-difference))).cache()
  spark.time(print(db.count()))


  def smallerL(x : Long, y : Long) : Boolean = {

    if (x < y) true
    else false
  }
  val C = new ComparisonJoins()
  val h1CoGroup = C.groupBy(h1, 2, 3, smallerL(_,_), smallerL(_,_)).map(x => (x._1._2, x))
    .groupByKey()
  val h2coGroup = h2.groupByKey().map(x => (x._1._2, x)).groupByKey()

  val result = h1CoGroup.cogroup(h2coGroup).mapPartitions(x => {
    val temp = new mutable.HashMap[(Long, Long), Int]()
    for (i <- x) {
      if (i._2._1.nonEmpty && i._2._2.nonEmpty) {
        if (i._2._1.size != 1 || i._2._2.size != 1) throw new Exception(s"Size error! ${i._2._1.size}, ${i._2._2.size}"  )
        val leftiter = i._2._1.head
        val rightiter = i._2._2.head
        for (j <- leftiter) {
            for (k <- rightiter) {
              breakable {
                if (k._1._1 != j._1._1) {
                  for (k1 <- k._2) {
                    if (j._2.exists(k1(2).asInstanceOf[Long], k1(3).asInstanceOf[Long])) {
                      val t = (j._1._1, k._1._1)
                      val l = temp.getOrElse(t, 0)
                      temp.put(t, l + 1)
                      break
                    }
                  }
                }
              }
          }
        }
      }
    }
    temp.toIterator
  })



  spark.time(println(result.reduceByKey((x, y) => x+y).count()))


  println("First SparkContext:")
  println("APP Name :" + spark.sparkContext.appName)
  println("Deploy Mode :" + spark.sparkContext.deployMode)
  println("Master :" + spark.sparkContext.master)

  spark.close()
}
