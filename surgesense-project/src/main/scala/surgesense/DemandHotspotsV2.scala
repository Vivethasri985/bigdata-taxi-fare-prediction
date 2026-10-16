package surgesense

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator

object DemandHotspotsV2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SurgeSense-Phase10-Demand-Hotspots-V2")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.parquet.enableVectorizedReader", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val inputPath  = "hdfs://namenode:9000/surgesense/processed/features"
    val modelPath  = "hdfs://namenode:9000/surgesense/models/demand_hotspots/kmeans_v2"
    val outputBase = "hdfs://namenode:9000/surgesense/results/demand_hotspots_v2"

    println("==================================================================")
    println("  SurgeSense Phase 10 V2: Improved Demand Hotspot Detection       ")
    println("  Changes: log1p(pickup_count) + demand-focused feature vector    ")
    println("==================================================================")

    // ============================================================
    // STEP 1: LOAD & AGGREGATE (identical to V1)
    // ============================================================
    println("\n[1] Loading processed taxi data and aggregating demand...")
    val rawTrips = spark.read.parquet(inputPath)

    val demandDF = rawTrips
      .filter(
        col("pickup_zone").isNotNull &&
        col("pickup_date").isNotNull &&
        col("pickup_hour").isNotNull
      )
      .groupBy(
        col("pickup_location_id"),
        col("pickup_zone"),
        col("pickup_borough"),
        col("pickup_date"),
        col("pickup_hour"),
        col("pickup_year"),
        col("pickup_month"),
        col("day_of_week"),
        col("is_weekend")
      )
      .agg(
        count("*").cast("double").alias("pickup_count"),
        coalesce(avg("temperature_2m"), lit(15.0)).alias("temperature_2m"),
        coalesce(avg("precipitation"), lit(0.0)).alias("precipitation")
      )
      .cache()

    val totalIntervals = demandDF.count()
    println(s"Total demand intervals: $totalIntervals")

    // ============================================================
    // STEP 2: LOG1P TRANSFORM + DEMAND-FOCUSED FEATURE VECTOR
    //
    // FIX 1: log1p(pickup_count) compresses the right-skewed tail.
    //         Without this, K-Means distances are dominated by the
    //         few high-volume zone-hours (e.g. 1072 vs avg 58),
    //         pulling centroids away from the true mass of data.
    //
    // FIX 2: Only use pickup_count + pickup_hour as features.
    //         In V1, adding day_of_week / is_weekend / weather
    //         diluted the demand signal — K-Means was clustering
    //         time patterns, not demand intensity.
    //
    // pickup_hour is kept because demand is strongly hour-dependent
    // (avg hotspot demand at 18:00 = 64.4 vs 05:00 = 7.1).
    // ============================================================
    println("\n[2] Applying log1p(pickup_count) + assembling demand features...")

    val preparedDF = demandDF
      .withColumn("log_pickup_count", log1p(col("pickup_count")))
      .withColumn("pickup_hour_dbl",  col("pickup_hour").cast("double"))

    // Feature vector: log-transformed demand + hour
    val featureCols = Array("log_pickup_count", "pickup_hour_dbl")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("raw_features")

    val assembled = assembler.transform(preparedDF)

    val scaler = new StandardScaler()
      .setInputCol("raw_features")
      .setOutputCol("scaled_features")
      .setWithMean(true)
      .setWithStd(true)

    val scalerModel = scaler.fit(assembled)
    val scaledDF = scalerModel.transform(assembled).cache()

    // ============================================================
    // STEP 3: K-SELECTION ON EVALUATION SAMPLE
    // ============================================================
    println("\n[3] Evaluating candidate K values (K = 2, 3, 4, 5, 6, 7, 8)...")

    val sampleFraction = math.min(1.0, 60000.0 / totalIntervals.toDouble)
    val evalSample = scaledDF.sample(false, sampleFraction, 42).cache()
    val evalCount  = evalSample.count()
    println(s"Evaluation sample: $evalCount intervals")

    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("scaled_features")
      .setPredictionCol("cluster")
      .setMetricName("silhouette")

    val kCandidates = Seq(2, 3, 4, 5, 6, 7, 8)
    var kResults    = Seq[(Int, Double)]()

    for (k <- kCandidates) {
      val t0 = System.nanoTime()
      val model = new KMeans()
        .setK(k).setSeed(42).setMaxIter(30)
        .setFeaturesCol("scaled_features")
        .setPredictionCol("cluster")
        .fit(evalSample)

      val sil  = evaluator.evaluate(model.transform(evalSample))
      val secs = (System.nanoTime() - t0) / 1e9
      println(f"  K=$k  Silhouette=$sil%.4f  ($secs%.1f s)")
      kResults = kResults :+ (k, sil)
    }

    evalSample.unpersist()

    val kResultsDF = kResults.toDF("k", "silhouette_score")
    kResultsDF.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(s"$outputBase/k_selection")

    println("\nK-Selection Table:")
    kResultsDF.show(false)

    val (optimalK, bestSil) = kResults.maxBy(_._2)
    println(s"==> Optimal K = $optimalK  (Silhouette = $bestSil%.4f)")

    // ============================================================
    // STEP 4: TRAIN FINAL K-MEANS ON FULL DATASET
    // ============================================================
    println(s"\n[4] Training final K-Means (K=$optimalK) on full dataset...")
    val t0 = System.nanoTime()
    val finalModel: KMeansModel = new KMeans()
      .setK(optimalK).setSeed(42).setMaxIter(50)
      .setFeaturesCol("scaled_features")
      .setPredictionCol("cluster")
      .fit(scaledDF)

    println(f"Training time: ${(System.nanoTime() - t0) / 1e9}%.2f seconds")
    finalModel.write.overwrite().save(modelPath)
    println(s"Model saved to: $modelPath")

    val clusteredDF = finalModel.transform(scaledDF).cache()

    // ============================================================
    // STEP 5: CLUSTER SUMMARY
    // ============================================================
    println("\n[5] Computing cluster profiles...")

    val clusterStats = clusteredDF
      .groupBy("cluster")
      .agg(
        count("*").alias("observation_count"),
        avg("pickup_count").alias("avg_pickups"),
        min("pickup_count").alias("min_pickups"),
        max("pickup_count").alias("max_pickups"),
        sum("pickup_count").alias("total_pickups"),
        stddev("pickup_count").alias("stddev_pickups"),
        avg("pickup_hour").alias("avg_hour"),
        avg("is_weekend").alias("weekend_ratio"),
        avg("temperature_2m").alias("avg_temperature"),
        avg("precipitation").alias("avg_precipitation")
      )
      .orderBy("avg_pickups")
      .cache()

    println("\n========== CLUSTER SUMMARY ==========")
    clusterStats.show(false)

    // Assign semantic demand labels sorted by avg_pickups
    val sortedClusters = clusterStats.select("cluster").collect()
      .map(_.getAs[Int]("cluster"))

    // Assign semantic demand labels sorted by avg_pickups ascending
    // sortedClusters(0) = lowest avg demand -> Low Demand
    // sortedClusters(last) = highest avg demand -> High Demand (Hotspot)
    // anything in between -> Moderate Demand
    val lowestCluster   = sortedClusters.head
    val highestCluster  = sortedClusters.last

    val clusterSummaryLabelled = clusterStats.withColumn(
      "demand_tier",
      when(col("cluster") === lit(highestCluster), "High Demand (Hotspot)")
      .when(col("cluster") === lit(lowestCluster),  "Low Demand")
      .otherwise("Moderate Demand")
    ).orderBy("avg_pickups")


    println("\n========== CLUSTER DEMAND TIERS ==========")
    clusterSummaryLabelled.show(false)

    clusterSummaryLabelled.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(s"$outputBase/cluster_summary")

    // ============================================================
    // STEP 6: HOTSPOT DRILL-DOWN ANALYTICS
    // ============================================================
    val hotspotClusterId = sortedClusters.last
    println(s"\n[6] Hotspot cluster = $hotspotClusterId. Running drill-down analytics...")

    val hotspotDF = clusteredDF.filter(col("cluster") === lit(hotspotClusterId)).cache()
    println(s"Hotspot intervals: ${hotspotDF.count()}")

    // Top Hotspot Zones
    val hotspotZones = hotspotDF
      .groupBy("pickup_zone", "pickup_borough")
      .agg(
        count("*").alias("hotspot_occurrences"),
        avg("pickup_count").alias("avg_hourly_demand"),
        sum("pickup_count").alias("total_hotspot_pickups"),
        max("pickup_count").alias("peak_hourly_demand")
      )
      .orderBy(desc("total_hotspot_pickups"))

    println("\n========== TOP 10 DEMAND HOTSPOT ZONES ==========")
    hotspotZones.show(10, false)
    hotspotZones.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(s"$outputBase/hotspot_zones")

    // Hourly Hotspot Distribution
    val hourlyHotspots = hotspotDF
      .groupBy("pickup_hour")
      .agg(
        count("*").alias("hotspot_occurrences"),
        avg("pickup_count").alias("avg_hourly_demand"),
        sum("pickup_count").alias("total_hotspot_pickups")
      )
      .orderBy("pickup_hour")

    println("\n========== HOURLY HOTSPOT DYNAMICS ==========")
    hourlyHotspots.show(24, false)
    hourlyHotspots.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(s"$outputBase/hourly_hotspots")

    // Weekday vs Weekend
    val weekendHotspots = hotspotDF
      .withColumn("day_type", when(col("is_weekend") === 1, "Weekend").otherwise("Weekday"))
      .groupBy("day_type")
      .agg(
        count("*").alias("hotspot_occurrences"),
        avg("pickup_count").alias("avg_hourly_demand"),
        sum("pickup_count").alias("total_hotspot_pickups")
      )

    println("\n========== WEEKDAY VS WEEKEND HOTSPOT DYNAMICS ==========")
    weekendHotspots.show(false)
    weekendHotspots.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(s"$outputBase/weekday_weekend")

    // Weather Impact
    val weatherHotspots = hotspotDF
      .withColumn("weather_condition",
        when(col("precipitation") > 0.0, "Precipitation").otherwise("Clear"))
      .groupBy("weather_condition")
      .agg(
        count("*").alias("hotspot_occurrences"),
        avg("pickup_count").alias("avg_hourly_demand"),
        sum("pickup_count").alias("total_hotspot_pickups")
      )

    println("\n========== WEATHER IMPACT ON HOTSPOTS ==========")
    weatherHotspots.show(false)
    weatherHotspots.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(s"$outputBase/weather_analysis")

    // Cleanup
    demandDF.unpersist()
    scaledDF.unpersist()
    clusteredDF.unpersist()
    hotspotDF.unpersist()
    clusterStats.unpersist()

    println("\n==================================================================")
    println("      PHASE 10 V2 DEMAND HOTSPOT DETECTION COMPLETED              ")
    println(s"All V2 outputs saved under: $outputBase")
    println("==================================================================")

    spark.stop()
  }
}
