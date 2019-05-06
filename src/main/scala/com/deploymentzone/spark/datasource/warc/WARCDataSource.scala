package com.deploymentzone.spark.datasource.warc

import java.time.format.DateTimeFormatter
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

object WARCDataSource {

  // https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/
  val schema = StructType(Seq(
    StructField("WARC-Type", StringType, false),
    StructField("WARC-Record-ID", StringType, false),
    StructField("WARC-Date", TimestampType, false),
    StructField("Content-Length", IntegerType, false),
    StructField("Content-Type", StringType, true),
    StructField("WARC-Concurrent-To", StringType, true),
    StructField("WARC-Block-Digest", StringType, true),
    StructField("WARC-Payload-Digest", StringType, true),
    StructField("WARC-IP-Address", StringType, true),
    StructField("WARC-Refers-To", StringType, true),
    StructField("WARC-Target-URI", StringType, true),
    StructField("WARC-Truncated", StringType, true),
    StructField("WARC-Warcinfo-ID", StringType, true),
    StructField("WARC-Concurrent-To", StringType, true),
    StructField("WARC-Filename", StringType, true),
    StructField("WARC-Profile", StringType, true),
    StructField("WARC-Identified-Payload-Type", StringType, true),
    StructField("WARC-Segment-Origin-ID", StringType, true),
    StructField("WARC-Segment-Number", StringType, true),
    StructField("WARC-Segment-Total-Length", StringType, true),
    StructField("Payload", StringType, true)
  ))

  val allColumnNames = schema.fields.map(_.name)

  val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
}

class WARCDataSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader =
    new WARCDataSourceReader(options.get("path").get())
}

class WARCDataSourceReader(paths: String)
  extends DataSourceReader {
  override def readSchema(): StructType = WARCDataSource.schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] =
    paths.split('\n').map(makePartition).toSeq.asJava

  private def makePartition(path: String): InputPartition[InternalRow] = WARCDataReaderFactory(path)
}

case class WARCDataReaderFactory(path: String) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new JWARCDataReader(path, WARCDataSource.allColumnNames)
}

