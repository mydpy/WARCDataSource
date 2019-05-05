package com.deploymentzone.spark.datasource.warc

import java.io.{BufferedReader, FileInputStream, IOException, InputStreamReader}
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
  var hasNext = true

  private def initializeReader(): BufferedReader = {
    val fileStream = path match {
      case p if p.endsWith(".gz") => new GZIPInputStream(new FileInputStream(path))
      case _ => new FileInputStream(path)
    }
    val decoder = new InputStreamReader(fileStream, StandardCharsets.UTF_8)
    new BufferedReader(decoder, 16384)
  }


  override def next(): Boolean = hasNext

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
      var readRecord = false

      breakable {
        while (true) {
          val line = bufferedReader.readLine()
          if (line == null) {
            hasNext = false
            break
          }

          line.trim() match {
            case "WARC/1.0" =>
              readRecord = true
            case ProcessHeader(label, value) if readRecord =>
              data.put(label, value)
            case "" if readRecord =>
              val contentLength = data.getOrElse("Content-Length", "0").toInt

              if (columns.contains("Payload")) {
                if (contentLength > 0) {
                  val buffer = new Array[Char](contentLength)
                  var offset: Int = 0
                  breakable {
                    do {
                      val bytesRead = bufferedReader.read(buffer, offset, contentLength - offset)
                      if (bytesRead == -1) {
                        hasNext = false
                        break
                      }
                      offset += bytesRead
                      println(s"$offset / $contentLength")
                    } while (offset < contentLength)
                  }

                  val str = new String(buffer)
                  println(s"string length: ${str.length}")
                  data.put("Payload", str)
                }
                readRecord = false
              } else {
                val skipped = bufferedReader.skip(contentLength)
                if (skipped < contentLength)
                  hasNext = false
              }
              break

              case "" =>
              case _ => println("UNEXPECTED: " + line)
            }
        }
      }

      val values = columns.map { column =>
        WARCDataSource.schema.apply(column).dataType match {
          case IntegerType => data.get(column).map(_.toInt).getOrElse(0)
          case TimestampType =>
            data.get(column).map(str => {
              val ldt = LocalDateTime.parse(str, WARCDataSource.dateTimeFormat)
              java.sql.Timestamp.valueOf(ldt)
            }).orNull
          case _ => data.get(column).orNull
        }
      }
      InternalRow.fromSeq(values)
    }

  }
}

