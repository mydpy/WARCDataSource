package com.deploymentzone.spark.datasource.warc

object Main extends InitializeSpark {

  def main(args: Array[String]): Unit = {
    val warc = spark.read.format("com.deploymentzone.spark.datasource.warc.WARCDataSource")
      .load("/Users/charles.feduke/Downloads/CC-MAIN-20190418101243-20190418122315-00032.warc.gz")
    warc.createOrReplaceTempView("warc")
    //val countsOfTypes = spark.sql("select `WARC-Type`, count(*) as cnt from warc group by 1")
    println(spark.sql("select count(*) from warc").collect().foreach(row => println(row.getAs[Long](0))))
    //println(countsOfTypes.collect().size)
      //.foreach(row => println(row.get(1).toString + ":\t" + row.getAs[Long]("cnt")))
  }

}
