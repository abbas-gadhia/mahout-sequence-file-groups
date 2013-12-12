package org.apache.mahout.classifier

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.Text
import java.lang.Iterable
import scala.collection.JavaConversions._
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.mahout.math.VectorWritable

/**
 * @author : Abbas Gadhia
 * @since : 4/12/13
 */
class CategoryReducer extends Reducer[Text, VectorWritable, Text, VectorWritable] {
  private val extractIdR = """^/(.+)?/(.+?):.+""".r
  private var multipleOutputs: MultipleOutputs[Text, VectorWritable] = null


  override def setup(context: Reducer[Text, VectorWritable, Text, VectorWritable]#Context) = {
    multipleOutputs = new MultipleOutputs[Text, VectorWritable](context)
  }

  override def reduce(key: Text, values: Iterable[VectorWritable], context: Reducer[Text, VectorWritable, Text, VectorWritable]#Context) = {
    val nKey = new Text

    for (v <- values) {
      extractIdR.findAllMatchIn(v.toString).foreach {
        m => nKey.set("/" + key + "/" + m.group(2))
      }

      // Here's where the magic of separating the groups into different files happens
      multipleOutputs.write(nKey, v, key.toString)
    }
  }

  override def cleanup(context: Reducer[Text, VectorWritable, Text, VectorWritable]#Context) = multipleOutputs.close()
}
