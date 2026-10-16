package surgesense

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

object Preprocess {

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession.builder()
        .appName("SurgeSense-Preprocessing")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.files.maxRecordsPerFile", "2000000")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val fs =
      FileSystem.get(
        spark.sparkContext.hadoopConfiguration
      )

    val tripsFolder =
      "hdfs://namenode:9000/surgesense/raw/trips"

    val zonesPath =
      "hdfs://namenode:9000/surgesense/raw/reference/taxi_zone_lookup.csv"

    val weatherFolder =
      "hdfs://namenode:9000/surgesense/raw/weather"

    val outputFolder =
      "hdfs://namenode:9000/surgesense/processed/features"

    println()
    println("====================================================")
    println("          SurgeSense Preprocessing")
    println("====================================================")
    println(s"Trips   : $tripsFolder")
    println(s"Zones   : $zonesPath")
    println(s"Weather : $weatherFolder")
    println(s"Output  : $outputFolder")
    println("====================================================")

    require(
      fs.exists(new Path(tripsFolder)),
      s"Taxi folder not found: $tripsFolder"
    )

    require(
      fs.exists(new Path(zonesPath)),
      s"Taxi zone file not found: $zonesPath"
    )

    require(
      fs.exists(new Path(weatherFolder)),
      s"Weather folder not found: $weatherFolder"
    )

    println()
    println("Loading taxi-zone lookup...")

    val zonesRaw =
      spark.read
        .option("header","true")
        .option("inferSchema","true")
        .csv(zonesPath)

    val zones =
      Enrich.prepareZones(zonesRaw)

    println("Taxi-zone lookup ready.")

    println()
    println("Loading hourly weather...")

    val weatherRaw =
      spark.read
        .option("multiLine","true")
        .json(weatherFolder)

    val weather =
      Enrich.prepareWeather(weatherRaw)

    println("Hourly weather ready.")

    val taxiFiles =
      fs.listStatus(new Path(tripsFolder))
        .filter(
          file =>
            file.getPath.getName
              .toLowerCase
              .endsWith(".parquet")
        )
        .sortBy(
          _.getPath.getName
        )

    println()
    println(
      s"Taxi parquet files detected: ${taxiFiles.length}"
    )

    if(taxiFiles.length != 185){
      println(
        s"WARNING: Expected 185 files but found ${taxiFiles.length}"
      )
    }

    val filenamePattern =
      """yellow_tripdata_([0-9]{4})-([0-9]{2})\.parquet""".r

    var completed = 0
    var failed = 0

    val failures =
      scala.collection.mutable.ArrayBuffer[String]()

    taxiFiles.zipWithIndex.foreach {

  case (file, index) =>

    file.getPath.getName match {

      case filenamePattern(yearText, monthText) =>

        val year = yearText.toInt
        val month = monthText.toInt

        val monthOutput =
          f"$outputFolder/source_year=$year/source_month=$month%02d"

        if (fs.exists(new Path(monthOutput))) {

          println(
            f"Skipping already processed: $year-$month%02d"
          )

        } else {

          println()
          println("====================================================")
          println(s"Processing ${index + 1}/${taxiFiles.length}")
          println(f"Dataset : $year-$month%02d")
          println(s"File    : ${file.getPath.getName}")
          println("====================================================")

          try {

            val raw =
              spark.read
                .option("mergeSchema", "false")
                .parquet(file.getPath.toString)

            println("1/4 Normalizing schema...")

            val normalized =
              Normalize(
                raw,
                year,
                month
              )

            println("2/4 Cleaning records...")

            val cleaned =
              Clean(normalized)

            println("3/4 Joining zones + weather...")

            val enriched =
              Enrich(
                cleaned,
                zones,
                weather
              )

            println("4/4 Engineering features...")

            val featured =
              Features(enriched)

            println("Writing final output:")
            println(monthOutput)

            featured
              .drop(
                "source_year",
                "source_month"
              )
              .coalesce(1)
              .write
              .mode("overwrite")
              .option(
                "compression",
                "snappy"
              )
              .parquet(monthOutput)

            completed += 1

            println(
              f"COMPLETED: $year-$month%02d"
            )

          } catch {

            case e: Exception =>

              failed += 1

              failures += file.getPath.getName

              println()
              println("**************** FAILED ****************")
              println(
                s"File    : ${file.getPath.getName}"
              )
              println(
                s"Reason  : ${e.getMessage}"
              )
              println("****************************************")
          }
        }

      case _ =>

        println(
          s"Skipping unexpected filename: ${file.getPath.getName}"
        )
    }
}


    println()
    println("====================================================")
    println("             PREPROCESSING SUMMARY")
    println("====================================================")

    println(
      s"Input files      : ${taxiFiles.length}"
    )

    println(
      s"Completed files  : $completed"
    )

    println(
      s"Failed files     : $failed"
    )

    println(
      s"Final output     : $outputFolder"
    )

    if(failures.nonEmpty){

      println()
      println("FAILED FILES:")

      failures.foreach{
        fileName =>
          println(" - " + fileName)
      }

    } else {

      println()
      println(
        "All taxi files processed successfully."
      )
    }

    println("====================================================")

    spark.stop()
  }
}