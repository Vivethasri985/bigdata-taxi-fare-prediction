package surgesense

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Normalize {

  private def findColumn(
      df: DataFrame,
      candidates: Seq[String]
  ): Option[String] = {

    val lookup =
      df.columns
        .map(c => c.toLowerCase -> c)
        .toMap

    candidates
      .map(_.toLowerCase)
      .find(lookup.contains)
      .map(lookup)
  }

  private def requiredColumn(
      df: DataFrame,
      candidates: Seq[String],
      dataType: DataType,
      outputName: String
  ): Column = {

    findColumn(df, candidates) match {

      case Some(actualName) =>
        col(actualName)
          .cast(dataType)
          .alias(outputName)

      case None =>
        throw new IllegalArgumentException(
          s"""
             |Required column '$outputName' was not found.
             |
             |Expected one of:
             |${candidates.mkString(", ")}
             |
             |Actual columns:
             |${df.columns.mkString(", ")}
             |""".stripMargin
        )
    }
  }

  private def optionalColumn(
      df: DataFrame,
      candidates: Seq[String],
      dataType: DataType,
      outputName: String
  ): Column = {

    findColumn(df, candidates) match {

      case Some(actualName) =>
        col(actualName)
          .cast(dataType)
          .alias(outputName)

      case None =>
        lit(null)
          .cast(dataType)
          .alias(outputName)
    }
  }

  def apply(
      raw: DataFrame,
      sourceYear: Int,
      sourceMonth: Int
  ): DataFrame = {

    raw.select(

      requiredColumn(
        raw,
        Seq(
          "tpep_pickup_datetime",
          "pickup_datetime",
          "trip_pickup_datetime",
          "Trip_Pickup_DateTime"
        ),
        TimestampType,
        "pickup_datetime"
      ),

      requiredColumn(
        raw,
        Seq(
          "tpep_dropoff_datetime",
          "dropoff_datetime",
          "trip_dropoff_datetime",
          "Trip_Dropoff_DateTime"
        ),
        TimestampType,
        "dropoff_datetime"
      ),

      optionalColumn(
        raw,
        Seq(
          "passenger_count",
          "Passenger_Count"
        ),
        DoubleType,
        "passenger_count"
      ),

      requiredColumn(
        raw,
        Seq(
          "trip_distance",
          "Trip_Distance"
        ),
        DoubleType,
        "trip_distance"
      ),

      requiredColumn(
        raw,
        Seq(
          "fare_amount",
          "Fare_Amt",
          "fare_amt"
        ),
        DoubleType,
        "fare_amount"
      ),

      optionalColumn(
        raw,
        Seq(
          "total_amount",
          "Total_Amt",
          "total_amt"
        ),
        DoubleType,
        "total_amount"
      ),

      optionalColumn(
        raw,
        Seq(
          "PULocationID",
          "pickup_location_id",
          "pu_location_id"
        ),
        LongType,
        "pickup_location_id"
      ),

      optionalColumn(
        raw,
        Seq(
          "DOLocationID",
          "dropoff_location_id",
          "do_location_id"
        ),
        LongType,
        "dropoff_location_id"
      ),

      optionalColumn(
        raw,
        Seq(
          "payment_type",
          "Payment_Type"
        ),
        StringType,
        "payment_type"
      ),

      // Older TLC files may use coordinates instead of LocationID.

      optionalColumn(
        raw,
        Seq(
          "pickup_longitude",
          "Start_Lon"
        ),
        DoubleType,
        "pickup_longitude"
      ),

      optionalColumn(
        raw,
        Seq(
          "pickup_latitude",
          "Start_Lat"
        ),
        DoubleType,
        "pickup_latitude"
      ),

      optionalColumn(
        raw,
        Seq(
          "dropoff_longitude",
          "End_Lon"
        ),
        DoubleType,
        "dropoff_longitude"
      ),

      optionalColumn(
        raw,
        Seq(
          "dropoff_latitude",
          "End_Lat"
        ),
        DoubleType,
        "dropoff_latitude"
      ),

      lit(sourceYear)
        .cast(IntegerType)
        .alias("source_year"),

      lit(sourceMonth)
        .cast(IntegerType)
        .alias("source_month")
    )
  }
}