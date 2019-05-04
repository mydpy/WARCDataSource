package com.deploymentzone.spark.datasource.warc

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.util.zip.GZIPInputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{IntegerType, TimestampType}

import scala.collection.mutable
import scala.util.control.Breaks._

class WARCDataReader(path: String, columns: Seq[String] = WARCDataSource.allColumnNames)
  extends InputPartitionReader[InternalRow] {

  lazy val bufferedReader: BufferedReader = initializeReader()

  private def initializeReader(): BufferedReader = {
    val fileStream = path match {
      case p if p.endsWith(".gz") => new GZIPInputStream(new FileInputStream(path))
      case _ => new FileInputStream(path)
    }
    val decoder = new InputStreamReader(fileStream, StandardCharsets.UTF_8)
    new BufferedReader(decoder, 16384)
  }


  override def next(): Boolean = bufferedReader.ready()

  override def get(): InternalRow = {
    Parser(bufferedReader).apply()
  }

  override def close(): Unit = bufferedReader.close()

  private case class Parser(reader: BufferedReader) {

    case object ProcessHeader {
      def unapply(line: String): Option[(String, String)] = line.split(":", 2) match {
        case Array(label, value) => Some((label, value.trim))
        case _ => None
      }
    }

    def apply(): InternalRow = {
      val data = mutable.Map.empty[String, String]

      breakable {
        while (bufferedReader.ready()) {
          val line = bufferedReader.readLine()

          line match {
            case "WARC/1.0" =>
            case "" =>
              val contentLength = data.getOrElse("Content-Length", "0").toInt

              if (contentLength > 0) {
                // the CRLF bytes have already been read
                val buffer = new Array[Char](contentLength)

                buffer.update(0, 13)
                buffer.update(1, 10)
                bufferedReader.read(buffer, 2, contentLength - 2)
                data.put("Payload", new String(buffer))
              }
              break
            case ProcessHeader(label, value) =>
              data.put(label, value)
          }

        }
      }

      val values = columns.map { column =>
        WARCDataSource.schema.apply(column).dataType match {
          case IntegerType => data(column).toInt
          case TimestampType =>
            val ldt = LocalDateTime.parse(data(column), WARCDataSource.dateTimeFormat)
            java.sql.Timestamp.valueOf(ldt)
          case _ => data.get(column).orNull
        }
      }
      InternalRow.fromSeq(values)
    }

  }
}

