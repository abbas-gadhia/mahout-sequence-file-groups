package org.apache.mahout.classifier

import org.apache.hadoop.fs.Path
import java.util.logging.Logger
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.{ToolRunner, Tool}
import org.apache.mahout.math.VectorWritable

/**
 * @author : Abbas Gadhia
 * @since : 3/12/13
 */
object SequenceFileGrouper {
  def main(args: Array[String]) = System.exit(ToolRunner.run(new SequenceFileGrouper(), args))
}

class SequenceFileGrouper extends Configured with Tool {
  val log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME)

  def run(args: Array[String]): Int = {
    if (args.length != 2) {
      log.severe("Please specify the input directory from which the sequence files will be read")
      -1
    }

    val job = new Job()

    {
      import job._
      setJarByClass(classOf[CategoryMapper])
      setJobName("Sequence File Grouper : Category Id")
      setMapperClass(classOf[CategoryMapper])
      setReducerClass(classOf[CategoryReducer])
      setMapOutputKeyClass(classOf[Text])
      setMapOutputValueClass(classOf[VectorWritable])
      setOutputKeyClass(classOf[Text])
      setOutputValueClass(classOf[VectorWritable])
      setInputFormatClass(classOf[SequenceFileInputFormat[Text, VectorWritable]])
      setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, VectorWritable]])
    }

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    if (job.waitForCompletion(true)) 0 else 1
  }
}
