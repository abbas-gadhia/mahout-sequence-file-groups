package org.apache.mahout.classifier

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.{LongWritable, Text}

/**
 * @author : Abbas Gadhia
 * @since : 3/12/13
 */
class CategoryMapper extends Mapper[Text, Text, LongWritable, Text] {
  override def map(key: Text, value: Text, context: Mapper[Text, Text, LongWritable, Text]#Context) = {
    val composite = key.toString
    val parts = composite.split("/")
    val category = new LongWritable(parts(1).toLong)

    // Prepending the unique id in the value. Will extract it out later in the reducer
    val newVal = new Text("/" + parts(2) + "/" + value.toString)
    context.write(category, newVal)
  }
}
