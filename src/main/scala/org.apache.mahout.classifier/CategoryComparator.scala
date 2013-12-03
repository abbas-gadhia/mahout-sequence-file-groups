package org.apache.mahout.classifier

import org.apache.hadoop.io.{WritableComparable, WritableComparator}

/**
 * @author : Abbas Gadhia
 * @since : 3/12/13
 */
class CategoryComparator extends WritableComparator(classOf[TextPair]) {
  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    val key1 = a.asInstanceOf[TextPair]
    val key2 = b.asInstanceOf[TextPair]
    key1.category.compareTo(key2.category)
  }
}
