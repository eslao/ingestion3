package dpla.eleanor.entries.harvest

import dpla.eleanor.Schemata
import dpla.eleanor.Schemata.MetadataType
import dpla.eleanor.harvesters.ContentHarvester
import dpla.eleanor.harvesters.opds2.Opds2Harvester
import dpla.ingestion3.dataStorage.OutputHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.time.Instant

object FulcrumHarvestEntry {

  def main(args: Array[String]): Unit = {
    val rootPath: String =
      if (args.isDefinedAt(0)) args(0)
      else System.getProperty("java.io.tmpdir")

    val performContentHarvest: Boolean =
      if (args.isDefinedAt(1)) args(1).toBoolean
      else true

    val conf = new SparkConf()
      .setAppName("Eleanor!")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.broadcastTimeout", "3600")
      .set("spark.driver.memory", "2GB") // increased driver memory
      .setMaster("local[1]") // runs on single executor

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // Sets the activity output path with timestamp
    val timestamp = new java.sql.Timestamp(Instant.now.getEpochSecond)
    val outputHelper: OutputHelper = new OutputHelper(
      rootPath,
      "fulcrum",
      "ebook-harvest",
      timestamp.toLocalDateTime
    )
    val harvestActivityPath = outputHelper.activityPath

    val metadataHarvester = new Opds2Harvester(
      timestamp,
      Schemata.SourceUri.Fulcrum,
      MetadataType.Opds2
    )

    val metadataDs = metadataHarvester.execute(
      spark = spark,
      feedUrl = "https://www.fulcrum.org/api/opds"
    )

    println(s"Harvested ${metadataDs.count()}")
    println(s"Writing to $harvestActivityPath")
    metadataDs
      .write
      .mode(SaveMode.Overwrite)
      .parquet(harvestActivityPath)

    if (performContentHarvest) {
      println(s"Harvesting content")
      val contentHarvester = new ContentHarvester()
      val contentDs = contentHarvester.harvestContent(metadataDs, spark)
      println(s"Writing content dataset to $harvestActivityPath")
      contentDs.write.mode(SaveMode.Overwrite).parquet(harvestActivityPath) // write to activity path
    }

    spark.stop()
  }

}
