package org.SparkCQC

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * This is a script that helps you to create Holding table from Trade table in TPC-E benchmark.
 */
object TradeToHold {
  def main(args: Array[String]): Unit = {
    val data = Source.fromFile(args(0))
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
    val result = new mutable.HashMap[(Long, String), (Long, String, Long, Long, Long)]()
    val output = ArrayBuffer[(Long, String, Long, Long)]()

    // TT_ID CA_ID S_SYBM T_DTS T_TRADE_PRICE
    val trade = new ArrayBuffer[(String, String, String, String, String, String)]()
    val tradeS = new ArrayBuffer[(String, String, String, String, String)]()
    val tradeB = new ArrayBuffer[(String, String, String, String, String)]()

    var current = 0L

    for (line <- data.getLines()) {
      current += 1
      if (current % 10000000 == 0)
        println("processing " + current)

      val temp = line.split("\\|")
      // T_ID, T_DTS, T_TT_ID, T_S_SYMB, T_TRADE_QTY, T_CA_ID, T_TRADE_PRICE
      val t = (temp(0).toLong, format.parse(temp(1)).getTime, temp(3), temp(5), temp(6).toLong, temp(8).toLong, temp(10).toDouble)
      if (t._3.contains("B")) {
        val tempR: (Long, String, Long, Long, Long) = result.getOrElse((t._6, t._4), (t._6, t._4, t._2, -1, 0))
        result.put((t._6, t._4), (tempR._1, tempR._2, tempR._3, tempR._4, tempR._5 + t._5))

        trade.append((temp(0), format.parse(temp(1)).getTime.toString, temp(3), temp(5), temp(8), temp(10)))
        tradeB.append((temp(0), format.parse(temp(1)).getTime.toString, temp(5), temp(8), temp(10)))
      } else {
        if (t._3.contains("S")) {
          val tempR: (Long, String, Long, Long, Long) = result.getOrElse((t._6, t._4), (t._6, t._4, t._2, -1, 0))
          if (tempR._5 - t._5 < 0) if (tempR._3 < t._2) output.append((tempR._1, tempR._2, tempR._3, t._2))
          if (tempR._5 - t._5 == 0) {
            output.append((tempR._1, tempR._2, tempR._3, t._2))
            result.remove((t._6, t._4))
          } else {
            result.put((t._6, t._4), (tempR._1, tempR._2, tempR._3, tempR._4, tempR._5 - t._5))
          }

          trade.append((temp(0), format.parse(temp(1)).getTime.toString, temp(3), temp(5), temp(8), temp(10)))
          tradeS.append((temp(0), format.parse(temp(1)).getTime.toString, temp(5), temp(8), temp(10)))
        } else {
          throw new Exception("Some record are not buy or sell")
        }
      }
    }
    for (i <- result) {
      output.append((i._2._1, i._2._2, i._2._3, 4102329600000L))
    }

    println("Holding.csv")
    val writeF = new PrintWriter(new File("Holding.csv"))
    for (x <- output) {
      val startDate = new Date(x._3)
      val endDate = new Date(x._4)


      writeF.write(s"${x._1}|${x._2}|${format.format(startDate)}|${format.format(endDate)}\n")
    }

    writeF.close()

    val tradeWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/tmp/trade.txt")))
    val tradeBWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/tmp/tradeB.txt")))
    val tradeSWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/tmp/tradeS.txt")))

    println("trade.txt")
    for (t <- trade) {
      tradeWriter.write(s"${t._1}|${t._2}|${t._3}|${t._4}|${t._5}|${t._6}")
      tradeWriter.newLine()
    }
    tradeWriter.flush()
    tradeWriter.close()

    println("tradeS.txt")
    for (t <- tradeS) {
      tradeSWriter.write(s"${t._1}|${t._2}|${t._3}|${t._4}|${t._5}")
      tradeSWriter.newLine()
    }
    tradeSWriter.flush()
    tradeSWriter.close()

    println("tradeB.txt")
    for (t <- tradeB) {
      tradeBWriter.write(s"${t._1}|${t._2}|${t._3}|${t._4}|${t._5}")
      tradeBWriter.newLine()
    }
    tradeBWriter.flush()
    tradeBWriter.close()
  }
}
