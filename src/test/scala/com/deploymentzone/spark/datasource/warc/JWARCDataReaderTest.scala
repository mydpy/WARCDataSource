package com.deploymentzone.spark.datasource.warc

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest._

class JWARCDataReaderTest extends FlatSpec {

  // due to platform line ending CR vs CRLF this test just makes sure things work by looking for the APR-2019
  // archive in ~/Downloads
  "A JWARCDataReader" should "read WARC rows from an archive" in {
    val path = s"${System.getProperty("user.home")}/Downloads/CC-MAIN-20190418101243-20190418122315-00032.warc.gz"
    val reader = new JWARCDataReader(path)
    assert(reader.next() === true)
    val row = reader.get()
    val gr = createGenericRow(row)
    // InternalRow vs. Row has different behavior - in SparkSQL things need to be wrapped in a UTF8String
    assert(gr.getAs[UTF8String]("WARC-Type").toString === "warcinfo")
    assert(gr.getAs[Timestamp]("WARC-Date") ===
      java.sql.Timestamp.valueOf(LocalDateTime.parse("2019-04-18T10:12:43Z", WARCDataSource.dateTimeFormat)).getTime * 1000)

    assert(gr.getAs[UTF8String]("WARC-Record-ID").toString === "<urn:uuid:445f0832-23e7-4d73-9c26-a2089f0edef9>")
    assert(gr.getAs[Int]("Content-Length") === 501)

    debugGenericRow(gr)
    assert(reader.next() === true)
    debugGenericRow(createGenericRow(reader.get()))
    assert(reader.next() === true)
    debugGenericRow(createGenericRow(reader.get()))
    assert(reader.next() === true)
    debugGenericRow(createGenericRow(reader.get()))
    assert(reader.next() === true)

  }

  private def createGenericRow(row: InternalRow): GenericRowWithSchema = {
    new GenericRowWithSchema(row.toSeq(WARCDataSource.schema).toArray, WARCDataSource.schema)
  }

  private def debugGenericRow(gr: GenericRowWithSchema): Unit = {
    gr.schema.fieldNames.map { fieldName =>
      val idx = gr.schema.fieldIndex(fieldName)
      val value = gr.get(idx)
      (fieldName, value)
    }.filter { case (name, _) => name != "Payload" }
      .filter { case (_, value) => value != null }
      .foreach { case (name, value) => println(s"$name: $value") }

    val payload = gr.getAs[UTF8String]("Payload")
    if (payload != null) {
      println("*** PAYLOAD ***")
      println(payload)
    } else {
      println("*** no payload")
    }
    println("----------------------")
  }

}
