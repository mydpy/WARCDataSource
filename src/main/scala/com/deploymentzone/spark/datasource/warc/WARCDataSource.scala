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
    StructField("WARC-Type", StringType),
    StructField("WARC-Record-ID", StringType),
    StructField("WARC-Date", TimestampType),
    StructField("Content-Length", IntegerType),
    StructField("Content-Type", StringType),
    StructField("WARC-Concurrent-To", StringType),
    StructField("WARC-Block-Digest", StringType),
    StructField("WARC-Payload-Digest", StringType),
    StructField("WARC-IP-Address", StringType),
    StructField("WARC-Refers-To", StringType),
    StructField("WARC-Target-URI", StringType),
    StructField("WARC-Truncated", StringType),
    StructField("WARC-Warcinfo-ID", StringType),
    StructField("WARC-Concurrent-To", StringType),
    StructField("WARC-Filename", StringType),
    StructField("WARC-Profile", StringType),
    StructField("WARC-Identified-Payload-Type", StringType),
    StructField("WARC-Segment-Origin-ID", StringType),
    StructField("WARC-Segment-Number", StringType),
    StructField("WARC-Segment-Total-Length", StringType),
    StructField("Payload", StringType)
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

