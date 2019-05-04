package com.deploymentzone.spark.datasource.warc

import java.io.{BufferedInputStream, FileInputStream}
import java.time.LocalDateTime

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.archive.io.warc.WARCReaderFactory

class WARCArchiveRecordDataReader(path: String, columns: Seq[String] = WARCDataSource.allColumnNames)
  extends InputPartitionReader[InternalRow] {

  private lazy val inputStream = new BufferedInputStream(new FileInputStream(path))
  private lazy val archiveReader = WARCReaderFactory.get(path, inputStream, true)
  private lazy val archiveRecordIterator = archiveReader.iterator()

  override def next(): Boolean = archiveRecordIterator.hasNext

  override def get(): InternalRow = {
    val archiveRecord = archiveRecordIterator.next()
    val fields = archiveRecord.getHeader.getHeaderFields
    val rawData = IOUtils.toByteArray(archiveRecord, archiveRecord.available)
    fields.put("Payload", new String(rawData))
    val values = columns.map { column =>
      WARCDataSource.schema.apply(column).dataType match {
        case IntegerType => fields.get(column).toString.toInt
        case TimestampType =>
          val ldt = LocalDateTime.parse(fields.get(column).toString, WARCDataSource.dateTimeFormat)
          java.sql.Timestamp.valueOf(ldt)
        case _ => fields.get(column)
      }
    }
    InternalRow.fromSeq(values)
  }

  override def close(): Unit = archiveReader.close()
}

