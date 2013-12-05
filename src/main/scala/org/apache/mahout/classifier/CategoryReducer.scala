package org.apache.mahout.classifier

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.{LongWritable, Text}
import java.lang.Iterable
import scala.collection.JavaConversions._
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

/**
 * @author : Abbas Gadhia
 * @since : 4/12/13
 */
class CategoryReducer extends Reducer[LongWritable, Text, Text, Text] {
  private val extractIdR = """^/(\d+)/(.*)""".r
  private var multipleOutputs: MultipleOutputs[Text, Text] = null


  override def setup(context: Reducer[LongWritable, Text, Text, Text]#Context) = {
    multipleOutputs = new MultipleOutputs[Text, Text](context)
  }

  override def reduce(key: LongWritable, values: Iterable[Text], context: Reducer[LongWritable, Text, Text, Text]#Context) = {
    val nKey = new Text
    val nVal = new Text

    for (v <- values) {
      extractIdR.findAllMatchIn(v.toString).foreach {
        m =>
          nKey.set(new Text("/" + key + "/" + m.group(1)))
          nVal.set(m.group(2))
      }

      multipleOutputs.write(nKey, nVal, key.toString)
    }
  }

  override def cleanup(context: Reducer[LongWritable, Text, Text, Text]#Context) = multipleOutputs.close()
}
