package com.availity.spark.provider


import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.{pretty, render}

import java.io.File
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object ProviderRoster {

  def main(args: Array[String]): Unit = {
    process()
  }

  def process(): Unit = {
    val spark = getSparkSession


    var providersDF = loadCSVToDF(spark, "data/providers.csv", "|","true")
    var visitsDF = loadCSVToDF(spark, "data/visits.csv", ",","false")
    visitsDF = renameColumns(visitsDF, Map("_c0" -> "visit_id"))
    visitsDF = renameColumns(visitsDF, Map("_c1" -> "provider_id"))
    visitsDF = renameColumns(visitsDF, Map("_c2" -> "date_of_service"))

    providersDF = combineNames(providersDF)

    providersDF.printSchema()
    visitsDF.printSchema()

    val visitsPerProviderDF = calculateVisitsPerProvider(providersDF, visitsDF)
    visitsPerProviderDF.show()


    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    visitsPerProviderDF
      .write
      .partitionBy("provider_specialty")
      .format("json")
      .option("multiline", true)
      .mode("overwrite")
      .json("data/visits_per_provider")

    val directoryPath = "data/visits_per_provider"
    val extension = ".crc"
    deleteFilesWithExtension(directoryPath, extension)

    val visitsPerProviderPerMonthDF = calculateVisitsPerProviderPerMonth(providersDF, visitsDF)
      .select("provider_id","month","total_visits")

    visitsPerProviderPerMonthDF.show()

    val json = visitsPerProviderPerMonthDF
      .toJSON
      .collect()
      .mkString("[", ",", "]")
    val parentNode = "visitsPerProviderPerMonth"
    val jsonWithParentNode = s"""{"$parentNode": $json}"""
    val prettyJson = pretty(render(JsonMethods.parse(jsonWithParentNode)))
    val filePath = "data/visits_per_provider_per_month.json"
    Files.write(Paths.get(filePath), prettyJson.getBytes(StandardCharsets.UTF_8))
  }

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .appName("Read CSV into DataFrame")
      .config("spark.master", "local")
      .getOrCreate()
  }

  def loadCSVToDF(spark: SparkSession, path: String, delimiter: String, isThereAHeader : String): DataFrame = {
    spark.read
      .option("header", isThereAHeader)
      .option("delimiter", delimiter)
      .csv(path)
  }

  def calculateVisitsPerProvider(providerDF: DataFrame, visitorDF: DataFrame): DataFrame = {
    val joinedData = providerDF.join(visitorDF, Seq("provider_id"))
    joinedData.groupBy("provider_id", "provider_name", "provider_specialty")
      .agg(count("*").alias("total_visits"))
  }

  def renameColumns(df: DataFrame, columnNames: Map[String, String]): DataFrame = {
    var renamedDF = df
    columnNames.foreach { case (existingName, newName) =>
      renamedDF = renamedDF.withColumnRenamed(existingName, newName)
    }
    renamedDF
  }

  def combineNames(df: DataFrame): DataFrame = {
    df.withColumn("provider_name", functions.concat(col("first_name"), lit(" "), col("middle_name"), lit(" "), col("last_name")))
  }

  def calculateVisitsPerProviderPerMonth(providerDF: DataFrame, visitsDF: DataFrame): DataFrame = {
    val joinedData = providerDF.join(visitsDF, Seq("provider_id"))
    val visitsPerProviderPerMonthDF = joinedData
      .withColumn("month", monthToName(month(col("date_of_service"))))
      .groupBy("provider_id", "month")
      .agg(count("*").alias("total_visits"))
    visitsPerProviderPerMonthDF
  }

  val monthToName: UserDefinedFunction = udf((month: Int) => {
    val months = Array("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
    months(month - 1)
  })


  def deleteFilesWithExtension(directoryPath: String, extension: String): Unit = {
   def deleteFiles(file: File): Unit = {
      if (file.isDirectory) {
        val subFiles = file.listFiles()
        subFiles.foreach(deleteFiles)
      } else {
        if (file.getName.endsWith(extension)) {
          file.delete()
          println(s"Deleted file: ${file.getPath}")
        }
      }
    }
    val directory = new File(directoryPath)
    if (directory.exists()) {
      deleteFiles(directory)
    } else {
      println(s"Directory not found: $directoryPath")
    }
  }


}


