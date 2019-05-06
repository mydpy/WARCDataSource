package com.deploymentzone.spark.datasource.warc

object Main extends InitializeSpark {

  def main(args: Array[String]): Unit = {
    val warc = spark.read.format("com.deploymentzone.spark.datasource.warc.WARCDataSource")
      .load(s"${System.getProperty("user.home")}/Downloads/CC-MAIN-20190418101243-20190418122315-00032.warc.gz").cache()
    warc.createOrReplaceTempView("warc")
    val singleRow = spark.sql("SELECT `WARC-Type`, `Content-Length`, `WARC-Date` FROM warc limit 5")
    singleRow.collect().foreach(println)

    val countsOfTypes = spark.sql("""select `WARC-Type`, count(*) as cnt from warc group by 1""")
    countsOfTypes.foreach(row =>
      println(row.getAs[String]("WARC-Type") + ":\t" + row.getAs[Long]("cnt")))

    val totalCount = spark.sql("select count(*) as cnt from warc")
    totalCount.collect().foreach(println)
  }

}
