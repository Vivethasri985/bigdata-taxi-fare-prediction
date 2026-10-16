package surgesense

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

object Phase1SplitAndEDA {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SurgeSense-Phase1-EDA-TemporalSplit")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.parquet.enableVectorizedReader", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputPath  = "hdfs://namenode:9000/surgesense/processed/features"
    val trainPath  = "hdfs://namenode:9000/surgesense/processed/split/train"
    val valPath    = "hdfs://namenode:9000/surgesense/processed/split/val"
    val testPath   = "hdfs://namenode:9000/surgesense/processed/split/test"

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    println()
    println("==================================================================")
    println("      SurgeSense Phase 1: EDA & Leak-Free Temporal Split          ")
    println("==================================================================")
    println(s"Reading from HDFS: $inputPath")

    require(fs.exists(new Path(inputPath)), s"Features path not found: $inputPath")

    val df = spark.read.parquet(inputPath).cache()
    val totalCount = df.count()

    println(s"\n[1] TOTAL RECORD COUNT: $totalCount rows")

    // -------------------------------------------------------------
    // 1. Schema & Null Rate Verification
    // -------------------------------------------------------------
    println("\n[2] VERIFYING NULL RATES IN CRITICAL MODEL FEATURES:")
    val checkColumns = Seq(
      "fare_amount", "trip_distance", "trip_duration_min", 
      "average_speed_mph", "pickup_borough", "temperature_2m", 
      "precipitation", "day_of_week", "pickup_year", "pickup_month"
    )

    checkColumns.filter(df.columns.contains).foreach { colName =>
      val nullCount = df.filter(col(colName).isNull || col(colName).isNaN).count()
      println(f"  - $colName%-20s : $nullCount nulls (${(nullCount.toDouble / totalCount) * 100}%.2f%%)")
    }

    // -------------------------------------------------------------
    // 2. EDA Summary Statistics
    // -------------------------------------------------------------
    println("\n[3] TARGET & TRIP DISTRIBUTION SUMMARY:")
    df.select("fare_amount", "trip_distance", "trip_duration_min", "average_speed_mph")
      .describe()
      .show()

    println("\n[4] TRIP VOLUME BY YEAR & MONTH:")
    df.groupBy("pickup_year", "pickup_month")
      .count()
      .orderBy("pickup_year", "pickup_month")
      .show(50, truncate = false)

    println("\n[5] VOLUME BY PICKUP BOROUGH:")
    df.groupBy("pickup_borough")
      .count()
      .orderBy(desc("count"))
      .show(10, truncate = false)

    // -------------------------------------------------------------
    // 3. Temporal Split Execution
    // -------------------------------------------------------------
    println("\n[6] EXECUTING TEMPORAL SPLIT (NO RANDOM FUTURE LEAKAGE):")
    // Training: 2022 to 2025
    val trainDF = df.filter(col("pickup_year") >= 2022 && col("pickup_year") <= 2025)
    // Validation: Early 2026 (Jan - Feb)
    val valDF   = df.filter(col("pickup_year") === 2026 && col("pickup_month") <= 2)
    // Test: Later 2026 (March onwards)
    val testDF  = df.filter(col("pickup_year") === 2026 && col("pickup_month") > 2)

    val trainCount = trainDF.count()
    val valCount   = valDF.count()
    val testCount  = testDF.count()

    println(f"  -> Train Set (2022-2025)      : $trainCount%,d rows (${(trainCount.toDouble / totalCount) * 100}%.1f%%)")
    println(f"  -> Validation Set (Jan-Feb 2026): $valCount%,d rows (${(valCount.toDouble / totalCount) * 100}%.1f%%)")
    println(f"  -> Test Set (Held-Out 2026)   : $testCount%,d rows (${(testCount.toDouble / totalCount) * 100}%.1f%%)")

    // Clean old destination folders if re-running
    Seq(trainPath, valPath, testPath).foreach { p =>
      val hdfsPath = new Path(p)
      if (fs.exists(hdfsPath)) {
        fs.delete(hdfsPath, true)
      }
    }

    // -------------------------------------------------------------
    // 4. Save to HDFS as Snappy Parquet
    // -------------------------------------------------------------
    println("\n[7] WRITING SPLITS TO HDFS...")
    trainDF.write.mode("overwrite").parquet(trainPath)
    println(s"  -> Saved Train dataset to: $trainPath")

    valDF.write.mode("overwrite").parquet(valPath)
    println(s"  -> Saved Val dataset to:   $valPath")

    testDF.write.mode("overwrite").parquet(testPath)
    println(s"  -> Saved Test dataset to:  $testPath")

    println("\n==================================================================")
    println("         PHASE 1 COMPLETE: READY FOR MODEL TRAINING               ")
    println("==================================================================")

    df.unpersist()
    spark.stop()
  }
}