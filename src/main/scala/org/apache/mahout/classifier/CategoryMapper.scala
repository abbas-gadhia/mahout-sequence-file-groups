package org.apache.mahout.classifier

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import org.apache.mahout.math.VectorWritable

/**
 * @author : Abbas Gadhia
 * @since : 3/12/13
 */
class CategoryMapper extends Mapper[Text, VectorWritable, Text, VectorWritable] {
  override def map(key: Text, value: VectorWritable, context: Mapper[Text, VectorWritable, Text, VectorWritable]#Context) = {
    val composite = key.toString
    // parts contains [,group-id, unique-id]
    val parts = composite.split("/")
    val category = new Text(parts(1))
    context.write(category, value)
  }
}
