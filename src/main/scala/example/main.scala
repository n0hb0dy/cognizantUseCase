package example

// import java.io.IOException
// import scala.util.Try
// // import os._

// import org.apache.spark.sql.SparkSession
// import java.nio.file.{Paths, Files}
// import scala.sys.process._

import org.apache.spark.sql.{Row, DataFrame}
// import org.apache.spark.sql.types._
// import org.apache.spark.sql.functions._
// import scala.collection.JavaConversions._
// import org.apache.spark.sql.expressions.Window

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.Path;
// import java.io.PrintWriter;
// import java.io._
// import scala.collection.mutable.ListBuffer
// import scala.io.Source

object main extends App {
  val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("Cognizant Use Case")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val mainSchema = new StructType()
    .add("RecordNumber", IntegerType, true)
    .add("Zipcode", IntegerType, true)
    .add("ZipCodeType", StringType, true)

  println("\nReading the data Set...\n")
  val csvDF = spark.read
    .format("csv")
    .option("header", true)
    .schema(mainSchema)
    .load("file:///home/maria_dev/SparkSamples/resources/zipcodes.csv")
  println("Done...\n")

  if (scala.io.StdIn.readLine("Store in Parquet? >> (y/n)").toLowerCase == "y")
    df.coalesce(1)
      .write
      .mode("overwrite")
      .Parquet(s"$folderPath")
}
