package surgesense

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Clean {

  def apply(df: DataFrame): DataFrame = {

    // ---------------------------------------------------------
    // Remove records missing mandatory trip information
    // ---------------------------------------------------------

    val c1 =
      df.na.drop(
        Seq(
          "pickup_datetime",
          "dropoff_datetime",
          "trip_distance",
          "fare_amount"
        )
      )

    // ---------------------------------------------------------
    // Fare validation
    // ---------------------------------------------------------

    val c2 =
      c1.filter(
        col("fare_amount") > 0.0 &&
        col("fare_amount") < 500.0
      )

    // ---------------------------------------------------------
    // Distance validation
    // ---------------------------------------------------------

    val c3 =
      c2.filter(
        col("trip_distance") > 0.0 &&
        col("trip_distance") < 100.0
      )

    // ---------------------------------------------------------
    // Passenger validation
    //
    // Null passenger count is retained because some historical
    // data may legitimately have it missing.
    // ---------------------------------------------------------

    val c4 =
      c3.filter(
        col("passenger_count").isNull ||
        col("passenger_count").between(1.0, 6.0)
      )

    // ---------------------------------------------------------
    // Pickup must occur before dropoff
    // ---------------------------------------------------------

    val c5 =
      c4.filter(
        col("pickup_datetime") <
        col("dropoff_datetime")
      )

    // ---------------------------------------------------------
    // Trip duration
    // ---------------------------------------------------------

    val c6 =
      c5.withColumn(
        "trip_duration_min",
        (
          col("dropoff_datetime").cast("long") -
          col("pickup_datetime").cast("long")
        ) / 60.0
      )

    // Maximum accepted duration = 4 hours.

    c6.filter(
      col("trip_duration_min") > 0.0 &&
      col("trip_duration_min") <= 240.0
    )
  }
}