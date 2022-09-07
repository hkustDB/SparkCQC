package org.SparkCQC

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random

object IOTaskHelper {
    /**
     *
     * @param result
     * @param configuredPath
     * @param parallelism
     * @tparam T
     */
    def saveResultAsTextFile[T: ClassTag](result: RDD[T], configuredPath: String, parallelism: Int): Unit = {
        assert(configuredPath.nonEmpty)
        var path = ""
        var isParallelIO = false
        if (configuredPath.startsWith("io")) {
            isParallelIO = false
            path = s"file:${configuredPath.substring(3)}"
        } else if (configuredPath.startsWith("pio")) {
            isParallelIO = true
            path = s"file:${configuredPath.substring(4)}"
        } else {
            throw new IllegalArgumentException("configuredPath should be 'io:/path/to/tmp_path' or 'pio:/path/to/tmp_path'")
        }

        if (isParallelIO) {
            // for parallel I/O Tasks, we maintain only (1 / parallelism) of the result and write it to disk,
            // since the result is always huge. We do it 4 times to mitigate the variation in the random filtering.
            // The paths will be '/path/to/tmp_path/tmp_x' where x in [1,2,3,4]
            for (i <- (1 to 4)) {
                result.mapPartitions(it => it.filter(_ => Random.nextInt(parallelism) == 0)).saveAsTextFile(s"${path}/tmp_${i}")
            }
        } else {
            // for normal I/O tasks, we just write all the results to disk
            result.saveAsTextFile(path)
        }
    }
}
