package org.apache.mahout.classifier

import java.net.{URI, URL}
import org.apache.hadoop.fs.{FileSystem, FsUrlStreamHandlerFactory}
import java.util.logging.Logger
import org.apache.hadoop.conf.Configuration

/**
 * @author : Abbas Gadhia
 * @since : 3/12/13
 */
object SequenceFileGrouper {
  URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
  val log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME)

  def main(args: Array[String]) {
    if (args.length != 1) {
      log.severe("Please specify the input directory from which the sequence files will be read")
      return
    }

    val uri = args(0)
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(uri), conf)
  }
}
