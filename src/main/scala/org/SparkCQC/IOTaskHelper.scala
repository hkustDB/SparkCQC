package org.SparkCQC

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object IOTaskHelper {
    /**
     * helper method for CQC jobs to save result as text file on disk.
     * It is part of the I/O tasks in the experiments.
     * @param result
     * @param configuredPath
     * @param parallelism
     * @tparam T
     */
    def saveResultAsTextFile[T: ClassTag](result: RDD[T], ioType: String, path: String, parallelism: Int): Unit = {
        assert(ioType == "normal_io" || ioType == "huge_io")
        val isNormalIo = (ioType == "normal_io")
        if (isNormalIo) {
            // for normal I/O tasks, we just write all the results to disk
            result.saveAsTextFile(s"file:${path}")
        } else {
            // for huge I/O Tasks, we maintain only (1 / parallelism) of the result and write it to disk,
            // since the result is often huge. And we will report the I/O cost after multiply by the parallelism.
            result.mapPartitions(it => new FilterIterator(it, parallelism)).saveAsTextFile(s"file:${path}")
        }
    }

    /**
     * A iterator wrapper that keeps one element every ${dropPeriod} elements.
     * e.g., after wrapping List(1,2,3,4,5,6,7).iterator with dropPeriod = 5, the FilterIterator will output 1 and 6
     * @param it
     * @param dropPeriod
     * @param classTag$T$0
     * @tparam T
     */
    class FilterIterator[T: ClassTag](it: Iterator[T], dropPeriod: Int) extends Iterator[T] {
        override def hasNext: Boolean = it.hasNext
        override def next(): T = {
            val result = it.next()
            it.drop(dropPeriod - 1)
            result
        }
    }
}
