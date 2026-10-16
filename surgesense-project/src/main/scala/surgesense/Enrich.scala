package surgesense

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Enrich {

  // ===========================================================
  // TAXI ZONE LOOKUP
  // ===========================================================

  def prepareZones(
      zonesRaw: DataFrame
  ): DataFrame = {

    zonesRaw.select(

      col("LocationID")
        .cast("long")
        .alias("pickup_location_id"),

      col("Borough")
        .alias("pickup_borough"),

      col("Zone")
        .alias("pickup_zone"),

      col("service_zone")
        .alias("pickup_service_zone")
    )
  }



  def prepareWeather(
      weatherRaw: DataFrame
  ): DataFrame = {

    val zipped =
      weatherRaw.select(

        arrays_zip(

          col("hourly.time"),

          col("hourly.temperature_2m"),

          col("hourly.precipitation"),

          col("hourly.wind_speed_10m"),

          col("hourly.weather_code")

        ).alias("hourly_weather")
      )

    val exploded =
      zipped
        .select(
          explode(
            col("hourly_weather")
          ).alias("weather")
        )

    exploded
      .select(

        col("weather.time")
          .alias("weather_time"),

        col("weather.temperature_2m")
          .cast("double")
          .alias("temperature_2m"),

        col("weather.precipitation")
          .cast("double")
          .alias("precipitation"),

        col("weather.wind_speed_10m")
          .cast("double")
          .alias("wind_speed_10m"),

        col("weather.weather_code")
          .cast("double")
          .alias("weather_code")
      )

      .withColumn(
        "weather_timestamp",
        to_timestamp(
          col("weather_time"),
          "yyyy-MM-dd'T'HH:mm"
        )
      )

      .withColumn(
        "weather_date",
        to_date(
          col("weather_timestamp")
        )
      )

      .withColumn(
        "weather_hour",
        hour(
          col("weather_timestamp")
        )
      )

      .drop(
        "weather_time",
        "weather_timestamp"
      )

      // Safety against accidental duplicate hourly records.
      .dropDuplicates(
        "weather_date",
        "weather_hour"
      )
  }

  // ===========================================================
  // ENRICH TAXI DATA
  // ===========================================================

  def apply(
      trips: DataFrame,
      zones: DataFrame,
      weather: DataFrame
  ): DataFrame = {

    // ---------------------------------------------------------
    // Taxi time keys
    // ---------------------------------------------------------

    val withTimeKeys =
      trips

        .withColumn(
          "pickup_date",
          to_date(
            col("pickup_datetime")
          )
        )

        .withColumn(
          "pickup_hour",
          hour(
            col("pickup_datetime")
          )
        )

    // ---------------------------------------------------------
    // TAXI ZONE JOIN
    // ---------------------------------------------------------

    val withZone =
      withTimeKeys.join(
        broadcast(zones),
        Seq("pickup_location_id"),
        "left"
      )

    

    val withWeather =
      withZone.join(
        broadcast(weather),

        withZone("pickup_date") ===
          weather("weather_date") &&

        withZone("pickup_hour") ===
          weather("weather_hour"),

        "left"
      )

    withWeather.drop(
      "weather_date",
      "weather_hour"
    )
  }
}