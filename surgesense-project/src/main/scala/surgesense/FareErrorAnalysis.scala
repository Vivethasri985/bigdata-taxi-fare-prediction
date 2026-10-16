package surgesense

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.regression.LinearRegressionModel

object FareErrorAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SurgeSense-Fare-Error-Analysis")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Original TEST data containing the individual predictor columns
    val testPath =
      "hdfs://namenode:9000/surgesense/processed/model_features/test"

    // Already-fitted preprocessing pipeline
    val encoderPath =
      "hdfs://namenode:9000/surgesense/models/feature_encoder"

    // Already-trained final LR model
    val modelPath =
      "hdfs://namenode:9000/surgesense/models/final_candidates/linear_regression"

    val outputBase =
      "hdfs://namenode:9000/surgesense/results/error_analysis"

    println("Loading original TEST dataset...")
    val test = spark.read.parquet(testPath)

    println("Loading fitted feature encoder...")
    val encoder = PipelineModel.load(encoderPath)

    println("Encoding TEST data using fitted encoder...")
    val encodedTest = encoder.transform(test)

    println("Loading final Linear Regression model...")
    val model = LinearRegressionModel.load(modelPath)

    println("Generating TEST predictions...")

    val predictions = model
      .transform(encodedTest)
      .withColumn(
        "error",
        col("prediction") - col("fare_amount")
      )
      .withColumn(
        "absolute_error",
        abs(col("error"))
      )
      .cache()

    val testRows = predictions.count()

    println(s"TEST rows: $testRows")

    // =========================================================
    // 1. OVERALL ERROR
    // =========================================================

    val overall = predictions
      .agg(
        avg("error").alias("mean_error"),
        avg("absolute_error").alias("mean_absolute_error"),
        expr("percentile_approx(absolute_error, 0.5)")
          .alias("median_absolute_error"),
        max("absolute_error").alias("maximum_absolute_error")
      )

    println()
    println("========== OVERALL ERROR ==========")
    overall.show(false)

    overall.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputBase/overall")

    // =========================================================
    // 2. ERROR BY FARE RANGE
    // =========================================================

    val fareAnalysis = predictions
      .withColumn(
        "fare_range",
        when(col("fare_amount") < 10, "<10")
          .when(col("fare_amount") < 20, "10-20")
          .when(col("fare_amount") < 30, "20-30")
          .when(col("fare_amount") < 50, "30-50")
          .otherwise("50+")
      )
      .groupBy("fare_range")
      .agg(
        count("*").alias("trips"),
        avg("fare_amount").alias("average_actual_fare"),
        avg("absolute_error").alias("mae"),
        sqrt(avg(pow(col("error"), 2))).alias("rmse")
      )
      .orderBy(
        when(col("fare_range") === "<10", 1)
          .when(col("fare_range") === "10-20", 2)
          .when(col("fare_range") === "20-30", 3)
          .when(col("fare_range") === "30-50", 4)
          .otherwise(5)
      )

    println()
    println("========== ERROR BY FARE RANGE ==========")
    fareAnalysis.show(false)

    fareAnalysis.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputBase/by_fare_range")

    // =========================================================
    // 3. ERROR BY TRIP DISTANCE
    // =========================================================

    val distanceAnalysis = predictions
      .withColumn(
        "distance_range",
        when(col("trip_distance") < 2, "<2 miles")
          .when(col("trip_distance") < 5, "2-5 miles")
          .when(col("trip_distance") < 10, "5-10 miles")
          .when(col("trip_distance") < 20, "10-20 miles")
          .otherwise("20+ miles")
      )
      .groupBy("distance_range")
      .agg(
        count("*").alias("trips"),
        avg("absolute_error").alias("mae"),
        sqrt(avg(pow(col("error"), 2))).alias("rmse")
      )
      .orderBy(
        when(col("distance_range") === "<2 miles", 1)
          .when(col("distance_range") === "2-5 miles", 2)
          .when(col("distance_range") === "5-10 miles", 3)
          .when(col("distance_range") === "10-20 miles", 4)
          .otherwise(5)
      )

    println()
    println("========== ERROR BY DISTANCE ==========")
    distanceAnalysis.show(false)

    distanceAnalysis.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputBase/by_distance")

    // =========================================================
    // 4. ERROR BY PICKUP HOUR
    // =========================================================

    val hourAnalysis = predictions
      .groupBy("pickup_hour")
      .agg(
        count("*").alias("trips"),
        avg("absolute_error").alias("mae"),
        sqrt(avg(pow(col("error"), 2))).alias("rmse")
      )
      .orderBy("pickup_hour")

    println()
    println("========== ERROR BY PICKUP HOUR ==========")
    hourAnalysis.show(24, false)

    hourAnalysis.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputBase/by_hour")

    // =========================================================
    // 5. ERROR BY DAY OF WEEK
    // =========================================================

    val dayAnalysis = predictions
      .groupBy("day_of_week")
      .agg(
        count("*").alias("trips"),
        avg("absolute_error").alias("mae"),
        sqrt(avg(pow(col("error"), 2))).alias("rmse")
      )
      .orderBy("day_of_week")

    println()
    println("========== ERROR BY DAY OF WEEK ==========")
    dayAnalysis.show(false)

    dayAnalysis.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputBase/by_day_of_week")

    // =========================================================
    // 6. WEEKDAY VS WEEKEND
    // =========================================================

    val weekendAnalysis = predictions
      .withColumn(
        "day_type",
        when(col("is_weekend") === 1, "Weekend")
          .otherwise("Weekday")
      )
      .groupBy("day_type")
      .agg(
        count("*").alias("trips"),
        avg("absolute_error").alias("mae"),
        sqrt(avg(pow(col("error"), 2))).alias("rmse")
      )

    println()
    println("========== WEEKDAY VS WEEKEND ==========")
    weekendAnalysis.show(false)

    weekendAnalysis.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputBase/by_day_type")

    // =========================================================
    // 7. ERROR BY WEATHER
    // =========================================================

    val weatherAnalysis = predictions
      .withColumn(
        "weather_condition",
        when(col("precipitation") > 0, "Precipitation")
          .otherwise("No precipitation")
      )
      .groupBy("weather_condition")
      .agg(
        count("*").alias("trips"),
        avg("absolute_error").alias("mae"),
        sqrt(avg(pow(col("error"), 2))).alias("rmse")
      )

    println()
    println("========== ERROR BY WEATHER ==========")
    weatherAnalysis.show(false)

    weatherAnalysis.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputBase/by_weather")

    println()
    println("==============================================")
    println("PHASE 9 FARE ERROR ANALYSIS COMPLETED")
    println(s"TEST rows analysed: $testRows")
    println(s"Results saved under: $outputBase")
    println("==============================================")

    predictions.unpersist()

    spark.stop()
  }
}