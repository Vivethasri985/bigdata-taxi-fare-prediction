package surgesense

import org.apache.spark.sql.SparkSession

object RunMLAndStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SurgeSense-ML-Training-and-Streaming")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.parquet.enableVectorizedReader", "false")
      .config("spark.ui.port", "4040")
      .config("spark.ui.bindAddress", "0.0.0.0")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("==================================================================")
    println(" SURGESENSE: UNIFIED ML TRAINING & STREAMING PIPELINE             ")
    println(" Port 4040: Capturing ML Optimization + Live Structured Stream    ")
    println("==================================================================")

    println("\n>>> [STEP 1/2] EXECUTING FINAL LINEAR REGRESSION TRAINING...")
    FinalLinearRegression.run(spark)

    println("\n>>> [STEP 2/2] STARTING STRUCTURED STREAMING INFERENCE ENGINE...")
    StreamingInference.run(spark)
  }
}
