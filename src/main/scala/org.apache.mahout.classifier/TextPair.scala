package org.apache.mahout.classifier

import org.apache.hadoop.io.{Text, WritableComparable}
import java.io.{DataOutput, DataInput}

/**
 * @author : Abbas Gadhia
 * @since : 3/12/13
 */
case class TextPair(category: Text, uniqueId: Text) extends WritableComparable[TextPair] {
  def readFields(in: DataInput) = {
    category.readFields(in)
    uniqueId.readFields(in)
  }

  def write(out: DataOutput) = {
    category.write(out)
    uniqueId.write(out)
  }

  def compareTo(o: TextPair): Int = {
    val cmp = category.compareTo(o.category)
    if (cmp != 0) {
      cmp
    }
    uniqueId.compareTo(o.uniqueId)
  }
}
