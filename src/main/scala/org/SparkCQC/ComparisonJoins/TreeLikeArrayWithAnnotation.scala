package org.SparkCQC.ComparisonJoins

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/**
  A 2-D data structure implemented based on TreeLike Array
 */
class TreeLikeArrayWithAnnotation[K1, K2, K3] extends java.io.Serializable {
  private var data_large : Array[(K1, Array[Array[Any]])] = _
  private var data_annotation : Array[(K1, Array[(K2, K3)])] = _
  private var key1 : Int = _
  private var key2 : Int = _
  private var smaller1 : (K1, K1) => Boolean = _
  private var smaller2 : (K2, K2) => Boolean = _
  private var largeEnumerator : LargeIterator = null
  private var annotation : Int = _
  private var sumAnnotation : (K3, K3) => K3 = _
  private var defaultAnnotation : K3 = _
  private def lowbit(x : Int) : Int = {
    x & -x
  }

  class LargeIterator() extends Iterator[Array[Any]] {
    private var k1 : K1 = _
    private var k2 : K2 = _
    private var iter : Int = 0
    private var hasOutput = false
    private var nextEle : Array[Any] = _
    private var pos : Int = _
    override def hasNext: Boolean = hasOutput

    override def next(): Array[Any] = {
      val result = nextEle
      var findNext = false
      while (pos > 0 && !findNext) {
        if (iter < data_large(pos)._2.length) {
          val j = data_large(pos)._2(iter)
          iter = iter + 1
          if (smaller2(j(key2).asInstanceOf[K2], k2)) {
            nextEle = j
            findNext = true
          }
        }
        if (!findNext) {
          pos = pos - lowbit(pos)
          iter = 0
        }
      }
      if (pos == 0) hasOutput = false
      result
    }

    def init(k1 : K1, k2 : K2) : Unit = {
      this.k1 = k1
      this.k2 = k2
      pos = TreeLikeArrayWithAnnotation.this.find(k1)
      hasOutput = true
      var findNext: Boolean = false
      while (pos > 0 && !findNext) {
        iter = 0
        if (iter < data_large(pos)._2.length) {
          val j = data_large(pos)._2(iter)
          iter = iter + 1
          if (smaller2(j(key2).asInstanceOf[K2], k2)) {
            nextEle = j
            findNext = true
          }
        }
        if (!findNext) pos = pos - lowbit(pos)
      }
    }
  }

  def this(ori: Array[Array[Any]],
           key1 : Int,
           key2 : Int,
           smaller1 : (K1, K1) => Boolean,
           smaller2 : (K2, K2) => Boolean,
           annotation : Int,
           sumAnnotation : (K3 , K3) => K3,
           defaultAnnotation : K3) {
    this()
    this.key1 = key1
    this.key2 = key2
    this.smaller1 = smaller1
    this.smaller2 = smaller2
    this.annotation = annotation
    this.sumAnnotation = sumAnnotation
    this.defaultAnnotation = defaultAnnotation
    val tempArray = ori.sortWith((x, y) => smaller1(x(key1).asInstanceOf[K1], y(key1).asInstanceOf[K1]))
    data_large = new Array[(K1, Array[Array[Any]])](tempArray.length+1)
    data_annotation = new Array[(K1, Array[(K2, K3)])](tempArray.length+1)
    for (i <- 1 to tempArray.length) {
      val s = lowbit(i)
      val sortedArray = tempArray
        .slice(i-s, i)
        .sortWith((x, y) => smaller2(x(key2).asInstanceOf[K2], y(key2).asInstanceOf[K2]))
      data_large(i) = (tempArray(i-1)(key1).asInstanceOf[K1], sortedArray)
      var tempagg = new Array[(K2, K3)](tempArray.length+1)
      var agg : K3 = defaultAnnotation
      for (j <- sortedArray.indices) {
        agg = sumAnnotation(agg, sortedArray(j)(annotation).asInstanceOf[K3])
        tempagg(j) = ((sortedArray(j)(key2).asInstanceOf[K2], agg))
      }
      data_annotation(i) = (tempArray(i-1)(key1).asInstanceOf[K1], tempagg)
    }
  }

  private def find(ele : K1) : Int = {
    var left = 1
    var right = data_large.length
    while (left < right) {
      val mid = (left + right)/2
      if (smaller1(data_large(mid)._1, ele)) {
        left = mid+1
      } else {
        right = mid
      }
    }
    left-1
  }

  def enumeration(k1 : K1, k2 : K2) : Array[Array[Any]] = {
    val tempArray = new ArrayBuffer[Array[Any]]()
    var i = find(k1)
    while (i > 0) {
      breakable {
        for (j <- data_large(i)._2) {
          if (smaller2(j(key2).asInstanceOf[K2], k2)) {
            tempArray.append(j)
          } else {
            break
          }
        }
      }
      i = i - lowbit(i)
    }
    tempArray.toArray
  }

  def findAnnotation(k1 : K1, k2 : K2) : K3 = {
    var i = find(k1)
    var agg = defaultAnnotation
    while (i > 0) {
      breakable {
        for (j <- data_annotation(i)._2) {
          if (!smaller2(j._1, k2)) {
            break
          }
          agg = j._2
        }
      }
    }
    agg
  }

  def enumerationIterator(k1 : K1, k2 : K2) : Iterator[Array[Any]] = {
    if (largeEnumerator == null) largeEnumerator = new LargeIterator
    largeEnumerator.init(k1, k2)
    largeEnumerator
  }

  def toSmall : Array[(K1, K2)] = {
    val size = data_large.length-1
    val tempArray = new Array[(K1, K2)](size)

    tempArray(0) = (data_large(1)._1,data_large(1)._2.head(key2).asInstanceOf[K2])
    for (i <- 1 until size) {
      if (smaller2(tempArray(i-1)._2, data_large(i+1)._2.head(key2).asInstanceOf[K2])) {
        tempArray(i) = (data_large(i+1)._1, tempArray(i-1)._2)
      } else {
        tempArray(i) = (data_large(i+1)._1, data_large(i+1)._2.head(key2).asInstanceOf[K2])
      }
    }
    tempArray
  }

  def exists(k1 : K1, k2 : K2) : Boolean = {
    val iter = enumerationIterator(k1, k2)
    if (iter.nonEmpty) true else false
  }
}
