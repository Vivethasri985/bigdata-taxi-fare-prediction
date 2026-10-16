package surgesense

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator

object FinalLinearRegression {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SurgeSense-Final-Linear-Regression")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    run(spark)
    spark.stop()
  }

  def run(spark: SparkSession): Unit = {
    spark.sparkContext.setLogLevel("WARN")

    val trainPath =
      "hdfs://namenode:9000/surgesense/processed/encoded/train"

    val valPath =
      "hdfs://namenode:9000/surgesense/processed/encoded/val"

    val modelPath =
      "hdfs://namenode:9000/surgesense/models/final_candidates/linear_regression"

    val resultPath =
      "hdfs://namenode:9000/surgesense/results/final_candidates/linear_regression"

    val train = spark.read.parquet(trainPath)
    val validation = spark.read.parquet(valPath)

    println("Training Linear Regression...")

    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("fare_amount")
      .setPredictionCol("prediction")
      .setMaxIter(50)
      .setRegParam(0.10)
      .setElasticNetParam(0.0)
      .setStandardization(true)

    val startTime = System.nanoTime()

    val model = lr.fit(train)

    val trainingSeconds =
      (System.nanoTime() - startTime) / 1e9

    println("Training completed.")
    println("Evaluating on official validation set...")

    val predictions = model
      .transform(validation)
      .select("fare_amount", "prediction")
      .cache()

    val validationRows = predictions.count()

    def evaluate(metric: String): Double = {
      new RegressionEvaluator()
        .setLabelCol("fare_amount")
        .setPredictionCol("prediction")
        .setMetricName(metric)
        .evaluate(predictions)
    }

    val rmse = evaluate("rmse")
    val mae = evaluate("mae")
    val r2 = evaluate("r2")

    val within2Count = predictions
      .filter(
        abs(col("prediction") - col("fare_amount")) <= 2.0
      )
      .count()

    val within2Accuracy =
      within2Count.toDouble / validationRows.toDouble * 100.0

    model.write
      .overwrite()
      .save(modelPath)

    import spark.implicits._

    val results = Seq(
      (
        "LinearRegression",
        rmse,
        mae,
        r2,
        within2Accuracy,
        trainingSeconds,
        validationRows,
        "maxIter=50,regParam=0.10,elasticNetParam=0.0,standardization=true"
      )
    ).toDF(
      "model",
      "rmse",
      "mae",
      "r2",
      "within_2_dollar_accuracy",
      "training_time_seconds",
      "validation_rows",
      "hyperparameters"
    )

    results.write
      .mode("overwrite")
      .option("header", "true")
      .csv(resultPath)

    println()
    println("========== LINEAR REGRESSION RESULTS ==========")
    results.show(false)

    println(s"Model saved: $modelPath")
    println(s"Results saved: $resultPath")

    predictions.unpersist()
  }
}