package surgesense

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Features {

  def apply(df: DataFrame): DataFrame = {

    // ---------------------------------------------------------
    // Temporal features
    // ---------------------------------------------------------

    val f1 =
      df

        .withColumn(
          "pickup_year",
          year(
            col("pickup_datetime")
          )
        )

        .withColumn(
          "pickup_month",
          month(
            col("pickup_datetime")
          )
        )

        .withColumn(
          "day_of_week",
          dayofweek(
            col("pickup_datetime")
          )
        )

        .withColumn(
          "is_weekend",
          when(
            dayofweek(
              col("pickup_datetime")
            ).isin(1, 7),
            1
          ).otherwise(0)
        )

    // ---------------------------------------------------------
    // Average speed
    // ---------------------------------------------------------

    val f2 =
      f1.withColumn(
        "average_speed_mph",

        col("trip_distance") /
        (
          col("trip_duration_min") /
          60.0
        )
      )

    // Reject unrealistic speeds.

    val f3 =
      f2.filter(
        col("average_speed_mph") > 0.0 &&
        col("average_speed_mph") <= 100.0
      )

    // ---------------------------------------------------------
    // Timestamp integrity
    //
    // Example:
    // 2024-03 file must actually contain March 2024 trips.
    // ---------------------------------------------------------

    f3.filter(
      col("pickup_year") ===
        col("source_year") &&

      col("pickup_month") ===
        col("source_month")
    )
  }
}