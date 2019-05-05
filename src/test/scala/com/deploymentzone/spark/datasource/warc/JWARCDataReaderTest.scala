package com.deploymentzone.spark.datasource.warc

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.scalatest._

class JWARCDataReaderTest extends FlatSpec {

  "A JWARCDataReader" should "read WARC rows from an archive" in {
    //val path = Thread.currentThread.getContextClassLoader.getResource("apr-2019-warc-partial.txt.gz").getPath
    val path = "/Users/charles.feduke/Downloads/CC-MAIN-20190418101243-20190418122315-00032.warc.gz"
    val reader = new JWARCDataReader(path)
    val row = reader.get()
    val gr = createGenericRow(row)
    assert(gr.getAs[String]("WARC-Type") === "warcinfo")
    assert(gr.getAs[Timestamp]("WARC-Date") ===
      java.sql.Timestamp.valueOf(LocalDateTime.parse("2019-04-18T10:12:43Z", WARCDataSource.dateTimeFormat)))

    assert(gr.getAs[String]("WARC-Record-ID") === "<urn:uuid:445f0832-23e7-4d73-9c26-a2089f0edef9>")
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

    val payload = gr.getAs[String]("Payload")
    if (payload != null) {
      println("*** PAYLOAD ***")
      println(gr.getAs[String]("Payload"))
    } else {
      println("*** no payload")
    }
    println("----------------------")
  }

}
