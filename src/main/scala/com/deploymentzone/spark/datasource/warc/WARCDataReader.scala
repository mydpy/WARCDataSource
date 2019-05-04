package com.deploymentzone.spark.datasource.warc

import java.io.{BufferedReader, FileInputStream, InputStream, InputStreamReader}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import java.io.BufferedReader
import java.util.zip.GZIPInputStream

import org.apache.spark.sql.Row

class WARCDataReader(path: String) extends InputPartitionReader[InternalRow] {

  lazy val bufferedReader: BufferedReader = initializeReader()

  private def initializeReader(): BufferedReader = {
    val fileStream = path match {
      case p if p.endsWith(".gz") => new GZIPInputStream(new FileInputStream(path))
      case _ => new FileInputStream(path)
    }
    val decoder = new InputStreamReader(fileStream, null)
    new BufferedReader(decoder)
  }


  override def next(): Boolean = bufferedReader.ready()

  override def get(): Row = {
    val line = bufferedReader.readLine()

  }

  override def close(): Unit = bufferedReader.close()
}

