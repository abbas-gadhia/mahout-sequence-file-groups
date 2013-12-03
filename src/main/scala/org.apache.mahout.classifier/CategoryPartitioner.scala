package org.apache.mahout.classifier

import org.apache.hadoop.mapreduce.Partitioner
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner

/**
 * @author : Abbas Gadhia
 * @since : 3/12/13
 */
class CategoryPartitioner extends Partitioner[TextPair, Text] {
  val hashPartitioner = new HashPartitioner[Text, Text]

  def getPartition(key: TextPair, value: Text, numPartitions: Int): Int = {
    hashPartitioner.getPartition(key.category, value, numPartitions)
  }
}
