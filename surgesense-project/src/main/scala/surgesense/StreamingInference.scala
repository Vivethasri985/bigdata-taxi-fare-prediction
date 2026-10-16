package surgesense

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.hadoop.fs.{FileSystem, Path}

object StreamingInference {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SurgeSense-Streaming-Inference")
      .config("spark.sql.files.ignoreMissingFiles", "true")
      .getOrCreate()
    run(spark)
  }

  def run(spark: SparkSession): Unit = {
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

    println("==================================================================")
    println("SurgeSense Phase 12: Distributed Stream Inference Engine")
    println("==================================================================")

    // ============================================================
    // 1. HDFS PATHS & CONFIGURATION
    // ============================================================
    val encoderPath       = "hdfs://namenode:9000/surgesense/models/feature_encoder"
    val lrModelPath       = "hdfs://namenode:9000/surgesense/models/final_candidates/linear_regression"
    val kmeansPath        = "hdfs://namenode:9000/surgesense/models/demand_hotspots/kmeans"

    val incomingPath      = "hdfs://namenode:9000/surgesense/streaming/incoming"
    val fareCheckpoint    = "hdfs://namenode:9000/surgesense/streaming/checkpoints/fare"
    val hotspotCheckpoint = "hdfs://namenode:9000/surgesense/streaming/checkpoints/hotspots"

    val liveFaresDir      = "hdfs://namenode:9000/surgesense/streaming/output/live_fares"
    val liveHotspotsDir   = "hdfs://namenode:9000/surgesense/streaming/output/live_hotspots"

    // ============================================================
    // 2. LOAD PRE-TRAINED MODELS (Immutable Artifacts)
    // ============================================================
    println("\n[1] Loading saved pipeline models from HDFS...")
    val featureEncoder = PipelineModel.load(encoderPath)
    val lrModel        = LinearRegressionModel.load(lrModelPath)
    val kmeansModel    = KMeansModel.load(kmeansPath)

    println("    Loaded: feature_encoder, linear_regression, kmeans (V2)")
    println(s"    KMeans featuresCol   = ${kmeansModel.getFeaturesCol}")
    println(s"    KMeans predictionCol = ${kmeansModel.getPredictionCol}")

    val kmeansPredCol = kmeansModel.getPredictionCol

    // ============================================================
    // 3. RAW INCOMING SCHEMA (Exact 21 Columns Contract)
    // ============================================================
    val rawSchema = new StructType()
      .add("pickup_datetime", TimestampType, nullable = true)
      .add("pickup_location_id", LongType, nullable = true)
      .add("dropoff_location_id", LongType, nullable = true)
      .add("pickup_borough", StringType, nullable = true)
      .add("pickup_zone", StringType, nullable = true)
      .add("pickup_service_zone", StringType, nullable = true)
      .add("passenger_count", DoubleType, nullable = true)
      .add("trip_distance", DoubleType, nullable = true)
      .add("payment_type", StringType, nullable = true)
      .add("trip_duration_min", DoubleType, nullable = true)
      .add("average_speed_mph", DoubleType, nullable = true)
      .add("pickup_year", IntegerType, nullable = true)
      .add("pickup_month", IntegerType, nullable = true)
      .add("pickup_hour", IntegerType, nullable = true)
      .add("day_of_week", IntegerType, nullable = true)
      .add("is_weekend", IntegerType, nullable = true)
      .add("temperature_2m", DoubleType, nullable = true)
      .add("precipitation", DoubleType, nullable = true)
      .add("wind_speed_10m", DoubleType, nullable = true)
      .add("weather_code", DoubleType, nullable = true)
      .add("fare_amount", DoubleType, nullable = true)

    // Snapshot schema for reading persistent hotspot state
    val hotspotSnapshotSchema = new StructType()
      .add("window_start", StringType, nullable = true)
      .add("window_end", StringType, nullable = true)
      .add("pickup_zone", StringType, nullable = true)
      .add("pickup_borough", StringType, nullable = true)
      .add("pickups_trailing_hour", LongType, nullable = true)
      .add("demand_tier", StringType, nullable = true)

    // ============================================================
    // 4. STRUCTURED STREAMING INGESTION SOURCE
    // ============================================================
    println(s"\n[2] Initializing Structured Streaming source: $incomingPath")
    val rawStream = spark.readStream
      .schema(rawSchema)
      .option("recursiveFileLookup", "true")
      .json(incomingPath)

    // ============================================================
    // 5. BRANCH 1: POINTWISE FARE INFERENCE STREAM
    // ============================================================
    println("\n[3] Setting up Branch 1: Pointwise Fare Inference stream...")
    // feature_encoder uses only its trained 19 predictors; fare_amount is preserved as ground truth
    val encodedStream = featureEncoder.transform(rawStream)
    val predictedFaresStream = lrModel.transform(encodedStream)

    val liveFares = predictedFaresStream.select(
      col("pickup_datetime").cast("string").alias("event_time"),
      col("pickup_zone"),
      col("dropoff_location_id"),
      round(col("trip_distance"), 2).alias("trip_distance"),
      col("passenger_count").cast("int").alias("passenger_count"),
      round(col("fare_amount"), 2).alias("actual_fare"),
      round(col("prediction"), 2).alias("predicted_fare"),
      round(abs(col("prediction") - col("fare_amount")), 2).alias("absolute_error")
    )

    val fareQuery = liveFares.writeStream
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .outputMode("append")
      .option("checkpointLocation", fareCheckpoint)
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          // Append events to cumulative live stream log
          batchDF.write.mode("append").json(liveFaresDir)

          // Atomically write recent snapshot (latest 25 trips for dashboard polling)
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          val snapshotPath = new Path(liveFaresDir + "_snapshot")
          val tmpPath      = new Path(liveFaresDir + "_snapshot_tmp")

          val snapshot = batchDF.limit(25)
          snapshot.coalesce(1).write.mode("overwrite").json(tmpPath.toString)
          if (fs.exists(snapshotPath)) {
            fs.delete(snapshotPath, true)
          }
          fs.rename(tmpPath, snapshotPath)

          val count = batchDF.count()
          println(s"  [Fare Stream Batch $batchId] Evaluated $count live trip predictions.")
        }
      }
      .start()

    // ============================================================
    // 6. BRANCH 2: STATEFUL WINDOWED HOTSPOT REGIME STREAM
    // ============================================================
    println("\n[4] Setting up Branch 2: Stateful Windowed Hotspot stream...")
    // Trailing 60-minute window sliding every 5 minutes with 10-minute watermark
    val windowedStream = rawStream
      .withWatermark("pickup_datetime", "10 minutes")
      .groupBy(
        window(col("pickup_datetime"), "1 hour", "5 minutes"),
        col("pickup_zone"),
        col("pickup_borough")
      )
      .agg(count("*").cast("double").alias("pickup_count"))

    val hotspotAssembler = new VectorAssembler()
      .setInputCols(Array("scaled_log_demand", "scaled_hour"))
      .setOutputCol("scaled_features")

    val hotspotQuery = windowedStream.writeStream
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .outputMode("update")
      .option("checkpointLocation", hotspotCheckpoint)
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          // Window end hour represents current trailing condition
          val prepared = batchDF
            .withColumn("pickup_hour_dbl", hour(col("window.end")).cast("double"))
            .withColumn("log_pickup_count", log1p(col("pickup_count")))
            // Standardize using fitted training parameters:
            // mu_log = 2.3512, std_log = 1.4589
            // mu_hour = 11.8919, std_hour = 6.56134
            .withColumn("scaled_log_demand", (col("log_pickup_count") - 2.3512) / 1.4589)
            .withColumn("scaled_hour", (col("pickup_hour_dbl") - 11.8919) / 6.56134)

          val assembled = hotspotAssembler.transform(prepared)
          val clustered = kmeansModel.transform(assembled)
            .withColumn("demand_tier",
              when(col(kmeansPredCol) === 1, "LOW DEMAND")
                .when(col(kmeansPredCol) === 0, "MODERATE DEMAND")
                .when(col(kmeansPredCol) === 2, "HIGH-DEMAND HOTSPOT")
                .otherwise("UNKNOWN")
            )

          val newUpdates = clustered.select(
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("pickup_zone"),
            col("pickup_borough"),
            col("pickup_count").cast("long").alias("pickups_trailing_hour"),
            col("demand_tier")
          )

          // State persistence across micro-batches:
          // In update mode, batchDF contains only newly updated zone windows.
          // We merge new updates with existing persistent snapshot to ensure non-updated active zones are preserved.
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          val snapshotPath = new Path(liveHotspotsDir + "_snapshot")
          val tmpPath      = new Path(liveHotspotsDir + "_snapshot_tmp")

          val mergedDF = if (fs.exists(snapshotPath)) {
            val existingState = spark.read.schema(hotspotSnapshotSchema).json(snapshotPath.toString)
            existingState.unionByName(newUpdates)
          } else {
            newUpdates
          }

          // Window ranking: For every zone, choose the latest window (maximum window_end)
          val latestPerZone = Window
            .partitionBy("pickup_zone")
            .orderBy(col("window_end").desc)

          val persistentSnapshot = mergedDF
            .withColumn("_rn", row_number().over(latestPerZone))
            .filter(col("_rn") === 1)
            .drop("_rn")
            .orderBy(col("pickups_trailing_hour").desc)
            .cache()

          val totalZones = persistentSnapshot.count()
          val numHotspots = persistentSnapshot.filter(col("demand_tier") === "HIGH-DEMAND HOTSPOT").count()

          // Atomically write updated full state to temporary path and swap into snapshot
          persistentSnapshot.coalesce(1).write.mode("overwrite").json(tmpPath.toString)
          if (fs.exists(snapshotPath)) {
            fs.delete(snapshotPath, true)
          }
          fs.rename(tmpPath, snapshotPath)

          persistentSnapshot.unpersist()

          println(s"  [Hotspots Batch $batchId] State merged: $totalZones active zones ($numHotspots high-demand hotspots).")
        }
      }
      .start()

    println("\n==================================================================")
    println("SurgeSense Streaming Engine is ACTIVE.")
    println(s"Listening for micro-batches in: $incomingPath")
    println("Queries running: Fare Prediction & Stateful Hotspots")
    println("==================================================================")

    spark.streams.awaitAnyTermination()
  }
}

