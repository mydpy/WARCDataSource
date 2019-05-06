package com.deploymentzone.spark.datasource.warc

import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.time.LocalDateTime
import java.util

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.netpreserve.jwarc.WarcReader
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class JWARCDataReader(path: String, columns: Seq[String] = WARCDataSource.allColumnNames)
  extends InputPartitionReader[InternalRow] {

  val logger = LoggerFactory.getLogger(classOf[JWARCDataReader])

  private val warcReader = initializeReader()
  private val iterator = warcReader.iterator()

  private def initializeReader(): WarcReader = {
    logger.info(s"Opening $path for WARC processing")
    new WarcReader(FileChannel.open(Paths.get(path)))
  }


  override def next(): Boolean = {
    try {
      iterator.hasNext
    } catch {
      // when the WARCReader encounters the EOF without a CRLFCRLF it throws an exception
      case _: java.io.UncheckedIOException => false
    }
  }

  override def get(): InternalRow = {
    val record = iterator.next()

    val originalData = record.headers().map().asScala
    val data = if (columns.contains("Payload")) {
      val bytes: Array[Byte] = IOUtils.toByteArray(record.body().stream(), record.body().size())
      val payload: String = new String(bytes, StandardCharsets.UTF_8)

      val data = mutable.Map(originalData.toSeq: _*)
      data.put("Payload", util.Collections.singletonList(payload))
      data
    } else {
      originalData
    }

    val values = columns.map { column =>
      WARCDataSource.schema.apply(column).dataType match {
        case IntegerType => data.get(column).map(_.get(0).toInt).getOrElse(0)
        case TimestampType =>
          // by default Spark SQL wants the Timestamp to be stored in seconds since 1-1-1979 0000
          data.get(column).map(str => {
            val ldt = LocalDateTime.parse(str.get(0), WARCDataSource.dateTimeFormat)
            java.sql.Timestamp.valueOf(ldt).getTime * 1000
          }).getOrElse(0L)
        case _ => UTF8String.fromString(data.get(column).map(_.get(0)).orNull)
      }
    }
    InternalRow.fromSeq(values)
  }

  override def close(): Unit = warcReader.close()

}

