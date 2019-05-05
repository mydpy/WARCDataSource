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
import org.netpreserve.jwarc.WarcReader

import scala.collection.JavaConverters._
import scala.collection.mutable

class JWARCDataReader(path: String, columns: Seq[String] = WARCDataSource.allColumnNames)
  extends InputPartitionReader[InternalRow] {

  private val warcReader = initializeReader()
  private lazy val iterator = warcReader.iterator()

  private def initializeReader(): WarcReader = {
    new WarcReader(FileChannel.open(Paths.get(path)))
  }


  override def next(): Boolean = iterator.hasNext

  override def get(): InternalRow = {
    val record = warcReader.next().get()

    val originalData = record.headers().map().asScala

    val bytes: Array[Byte] = IOUtils.toByteArray(record.body().stream(), record.body().size())
    val payload = new String(bytes, StandardCharsets.UTF_8)

    val data = mutable.Map(originalData.toSeq:_*)
    data.put("Payload", util.Collections.singletonList(payload))

    val values = columns.map { column =>
      WARCDataSource.schema.apply(column).dataType match {
        case IntegerType => data.get(column).map(_.get(0).toInt).getOrElse(0)
        case TimestampType =>
          data.get(column).map(str => {
            val ldt = LocalDateTime.parse(str.get(0), WARCDataSource.dateTimeFormat)
            java.sql.Timestamp.valueOf(ldt)
          }).orNull
        case _ => data.get(column).map(_.get(0)).orNull
      }
    }
    return InternalRow.fromSeq(values)
  }

  override def close(): Unit = warcReader.close()

}

