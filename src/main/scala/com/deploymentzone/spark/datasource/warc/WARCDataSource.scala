package com.deploymentzone.spark.datasource.warc

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import java.util.Optional

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object WARCDataSource {

  val schema = StructType(Seq(
    StructField("WARC-Type", StringType),
    StructField("WARC-Date", TimestampType),
    StructField("WARC-Record-ID", StringType)
  ))

}

class WARCDataSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader =
    new WARCDataSourceReader(options.get("paths").get())
}

class WARCDataSourceReader(paths: String)
  extends DataSourceReader {
  override def readSchema(): StructType = WARCDataSource.schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] =
    paths.split('\n').map(makePartition).toSeq.asJava

  private def makePartition(path: String): InputPartition[InternalRow] = WARCDataReaderFactory(path)
}

case class WARCDataReaderFactory(path: String) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = new WARCDataReader(path)
}

class WARCDataReader(path: String) extends InputPartitionReader[InternalRow] {
  override def next(): Boolean = ???

  override def get(): Row = ???

  override def close(): Unit = ???
}