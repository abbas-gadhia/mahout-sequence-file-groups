package org.apache.mahout.classifier

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text

/**
 * We're doing the split of the key in one place instead of doing it in the Partitioner and the Comparator
 *
 * @author : Abbas Gadhia
 * @since : 3/12/13
 */
class CategoryMapper extends Mapper[Text, Text, TextPair, Text] {
  override def map(key: Text, value: Text, context: Mapper[Text, Text, TextPair, Text]#Context) = {
    val composite = key.toString
    val parts = composite.split("/")
    val category = new Text(parts(1))
    val id = new Text(parts(2))
    context.write(new TextPair(category, id), value)
  }
}
