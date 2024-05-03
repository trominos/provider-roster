package com.availity.spark.provider

import com.availity.spark.provider.ProviderRoster.{calculateVisitsPerProvider, calculateVisitsPerProviderPerMonth, combineNames, renameColumns}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class ProviderRosterSpec extends AnyFunSpec with DataFrameComparer with BeforeAndAfterEach {

  private var spark: SparkSession = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
  }

  override def afterEach(): Unit = {
    spark.stop()
    super.afterEach()
  }

  describe("process") {
    it("should load providers and visits data and calculate visits per provider") {
      var providersDF = ProviderRoster.loadCSVToDF(spark, "data/providers.csv", "|", "true")
      var visitsDF = ProviderRoster.loadCSVToDF(spark, "data/visits.csv", ",", "false")
      visitsDF = renameColumns(visitsDF, Map("_c0" -> "visit_id"))
      visitsDF = renameColumns(visitsDF, Map("_c1" -> "provider_id"))
      visitsDF = renameColumns(visitsDF, Map("_c2" -> "date_of_service"))


      providersDF = combineNames(providersDF)
      val visitsPerProviderDF = calculateVisitsPerProvider(providersDF, visitsDF)
        .select("provider_specialty","provider_id","total_visits")
      val filteredDF = visitsPerProviderDF
        .filter(col("provider_specialty") === "Cardiology" && col("provider_id") === "21227")
      val totalVisits = filteredDF.select("total_visits").collect()(0)(0).asInstanceOf[Long]

      // Test total number of visits per provider
      assert(totalVisits == 29)


      val visitsPerProviderPerMonthDF = calculateVisitsPerProviderPerMonth(providersDF, visitsDF)
        .select("provider_id","month","total_visits").filter(col("provider_id") === "31993" && col("month") === "Jun")

      // Test calculate the total number of visits per provider per month
      val totalVisits_1 = visitsPerProviderPerMonthDF.select("total_visits").collect()(0)(0).asInstanceOf[Long]
      assert(totalVisits_1 == 2)
    }
  }
}
