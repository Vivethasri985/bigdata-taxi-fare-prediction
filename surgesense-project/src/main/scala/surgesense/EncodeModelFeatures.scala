package surgesense

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{
  StringIndexer,
  OneHotEncoder,
  VectorAssembler,
  Imputer
}
import org.apache.spark.ml.linalg.Vector

object EncodeModelFeatures {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SurgeSense-Encode-Model-Features")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ============================================================
    // Input paths
    // ============================================================

    val inputBase =
      "hdfs://namenode:9000/surgesense/processed/model_features"

    val trainPath = s"$inputBase/train"
    val valPath   = s"$inputBase/val"
    val testPath  = s"$inputBase/test"

    // ============================================================
    // Output paths
    // ============================================================

    val outputBase =
      "hdfs://namenode:9000/surgesense/processed/encoded"

    val trainOutPath = s"$outputBase/train"
    val valOutPath   = s"$outputBase/val"
    val testOutPath  = s"$outputBase/test"

    val encoderModelPath =
      "hdfs://namenode:9000/surgesense/models/feature_encoder"

    // ============================================================
    // Load datasets
    // ============================================================

    println("Loading model-feature datasets...")

    val trainDF = spark.read.parquet(trainPath)
    val valDF   = spark.read.parquet(valPath)
    val testDF  = spark.read.parquet(testPath)

    // ============================================================
    // Categorical predictors
    // ============================================================

    val categoricalColumns = Array(
      "pickup_location_id",
      "dropoff_location_id",
      "pickup_borough",
      "pickup_zone",
      "pickup_service_zone",
      "payment_type"
    )

    // ============================================================
    // Numerical predictors
    // ============================================================

    val numericalColumns = Array(
      "passenger_count",
      "trip_distance",
      "trip_duration_min",
      "average_speed_mph",
      "pickup_year",
      "pickup_month",
      "pickup_hour",
      "day_of_week",
      "is_weekend",
      "temperature_2m",
      "precipitation",
      "wind_speed_10m",
      "weather_code"
    )

    // ============================================================
    // Numerical imputation
    //
    // Missing numerical values are replaced using medians learned
    // ONLY from TRAIN.
    //
    // The same learned medians are then applied to VAL and TEST.
    // ============================================================

    val imputedNumericalColumns =
      numericalColumns.map(columnName => s"${columnName}_imputed")

    val imputer = new Imputer()
      .setInputCols(numericalColumns)
      .setOutputCols(imputedNumericalColumns)
      .setStrategy("median")

    // ============================================================
    // StringIndexer
    //
    // Converts categorical predictors into category indices.
    //
    // handleInvalid("keep") safely handles unseen categories in
    // validation/test.
    // ============================================================

    val indexers = categoricalColumns.map { columnName =>

      new StringIndexer()
        .setInputCol(columnName)
        .setOutputCol(s"${columnName}_idx")
        .setHandleInvalid("keep")
    }

    // ============================================================
    // One-Hot Encoder
    // ============================================================

    val indexedColumns =
      categoricalColumns.map(columnName => s"${columnName}_idx")

    val encodedColumns =
      categoricalColumns.map(columnName => s"${columnName}_ohe")

    val oneHotEncoder = new OneHotEncoder()
      .setInputCols(indexedColumns)
      .setOutputCols(encodedColumns)
      .setHandleInvalid("keep")
      .setDropLast(false)

    // ============================================================
    // VectorAssembler
    //
    // Combines imputed numerical predictors and one-hot encoded
    // categorical predictors into a single Spark ML feature vector.
    //
    // IMPORTANT:
    // Use IMPUTED numerical columns, not the original numerical
    // columns.
    // ============================================================

    val assemblerInputs =
      imputedNumericalColumns ++ encodedColumns

    val assembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")
      .setHandleInvalid("error")

    // ============================================================
    // Complete preprocessing pipeline
    //
    // 1. Median imputation
    // 2. String indexing
    // 3. One-hot encoding
    // 4. Feature-vector assembly
    // ============================================================

    val pipelineStages =
      Array(imputer) ++
      indexers ++
      Array(oneHotEncoder, assembler)

    val pipeline = new Pipeline()
      .setStages(pipelineStages)

    // ============================================================
    // Fit preprocessing ONLY on TRAIN
    // ============================================================

    println()
    println("============================================================")
    println("Fitting preprocessing pipeline on TRAIN only...")
    println("============================================================")

    val preprocessingModel: PipelineModel =
      pipeline.fit(trainDF)

    println("Preprocessing fitting completed.")

    // ============================================================
    // Save fitted preprocessing model
    //
    // This saves:
    // - TRAIN-derived medians
    // - StringIndexer mappings
    // - OneHotEncoder configuration
    // - VectorAssembler configuration
    // ============================================================

    preprocessingModel.write
      .overwrite()
      .save(encoderModelPath)

    println(s"Preprocessing model saved to: $encoderModelPath")

    // ============================================================
    // Transform TRAIN / VAL / TEST using SAME fitted pipeline
    // ============================================================

    println()
    println("Transforming TRAIN...")

    val encodedTrain =
      preprocessingModel.transform(trainDF)

    println("Transforming VALIDATION...")

    val encodedVal =
      preprocessingModel.transform(valDF)

    println("Transforming TEST...")

    val encodedTest =
      preprocessingModel.transform(testDF)

    // ============================================================
    // Save encoded datasets to HDFS
    // ============================================================
    println()
    println("Writing encoded datasets to HDFS...")

    encodedTrain.write
      .mode("overwrite")
      .parquet(trainOutPath)

    println(s"Encoded TRAIN saved to: $trainOutPath")

    encodedVal.write
      .mode("overwrite")
      .parquet(valOutPath)

    println(s"Encoded VALIDATION saved to: $valOutPath")

    encodedTest.write
      .mode("overwrite")
      .parquet(testOutPath)

    println(s"Encoded TEST saved to: $testOutPath")

    println()
    println("============================================================")
    println("      ENCODING PIPELINE EXECUTION COMPLETED")
    println("============================================================")
    println(s"Total features in vector: ${assemblerInputs.length}")
    println("Schema of encoded output:")
    encodedTrain.select("features", "fare_amount").printSchema()

    spark.stop()
  }
}
