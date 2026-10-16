package surgesense

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FinalModelFeatures {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SurgeSense-Final-Model-Features")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.parquet.enableVectorizedReader", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ============================================================
    // Input paths - existing temporal splits
    // ============================================================

    val trainPath =
      "hdfs://namenode:9000/surgesense/processed/split/train"

    val valPath =
      "hdfs://namenode:9000/surgesense/processed/split/val"

    val testPath =
      "hdfs://namenode:9000/surgesense/processed/split/test"

    // Separate output - original split data is NOT modified
    val outputBase =
      "hdfs://namenode:9000/surgesense/processed/model_features"

    // ============================================================
    // Final predictors
    // ============================================================

    val predictorColumns = Seq(

      // Geographic
      "pickup_location_id",
      "dropoff_location_id",
      "pickup_borough",
      "pickup_zone",
      "pickup_service_zone",

      // Trip
      "passenger_count",
      "trip_distance",
      "payment_type",
      "trip_duration_min",
      "average_speed_mph",

      // Temporal
      "pickup_year",
      "pickup_month",
      "pickup_hour",
      "day_of_week",
      "is_weekend",

      // Weather
      "temperature_2m",
      "precipitation",
      "wind_speed_10m",
      "weather_code"
    )

    val targetColumn = "fare_amount"

    val finalColumns = predictorColumns :+ targetColumn

    // ============================================================
    // Function to prepare each split
    // ============================================================

    def prepare(inputPath: String, outputPath: String, name: String): Unit = {

      println()
      println(s"================ $name ================")

      val df = spark.read.parquet(inputPath)

      // Make sure every required column exists
      val missingColumns =
        finalColumns.filterNot(df.columns.contains)

      require(
        missingColumns.isEmpty,
        s"$name is missing columns: ${missingColumns.mkString(", ")}"
      )

      // Select only final modelling columns
      val modelDF = df.select(finalColumns.map(col): _*)

      println(s"$name predictor count = ${predictorColumns.length}")
      println(s"$name total columns   = ${modelDF.columns.length}")

      // Write model-feature dataset
      modelDF.write
        .mode("overwrite")
        .parquet(outputPath)

      println(s"$name saved to: $outputPath")
    }

    // ============================================================
    // Prepare TRAIN / VALIDATION / TEST
    // ============================================================

    prepare(
      trainPath,
      s"$outputBase/train",
      "TRAIN"
    )

    prepare(
      valPath,
      s"$outputBase/val",
      "VALIDATION"
    )

    prepare(
      testPath,
      s"$outputBase/test",
      "TEST"
    )

    // ============================================================
    // Display final schema
    // ============================================================

    val finalTrain =
      spark.read.parquet(s"$outputBase/train")

    println()
    println("============================================================")
    println("             FINAL MODEL FEATURE SCHEMA")
    println("============================================================")

    finalTrain.printSchema()

    println(s"Predictors    = ${predictorColumns.length}")
    println(s"Target        = $targetColumn")
    println(s"Total columns = ${finalColumns.length}")

    println()
    println("Predictor columns:")

    predictorColumns.foreach(println)

    println()
    println("Excluded from modelling:")
    println("pickup_longitude    -> 100% NULL")
    println("pickup_latitude     -> 100% NULL")
    println("dropoff_longitude   -> 100% NULL")
    println("dropoff_latitude    -> 100% NULL")
    println("total_amount        -> target leakage")
    println("pickup_datetime     -> represented by engineered temporal features")
    println("dropoff_datetime    -> represented by trip_duration_min")
    println("pickup_date         -> represented by engineered temporal features")
    println("source_year         -> source metadata; pickup_year retained")
    println("source_month        -> source metadata; pickup_month retained")

    println()
    println("Final model-feature preparation completed successfully.")
    println("============================================================")

    spark.stop()
  }
}