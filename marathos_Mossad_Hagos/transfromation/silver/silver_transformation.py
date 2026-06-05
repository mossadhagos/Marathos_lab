from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.table(
    name="silver.results_obt",
    comment="Cleaned one-big-table of ultra-marathon results.",
)
def results_obt():
    df = spark.readStream.table("marathos.bronze.results_raw")

    rename_map = {
        "Year of event":              "year_of_event",
        "Event dates":                "event_dates",
        "Event name":                 "event_name",
        "Event distance/length":      "event_distance_length",
        "Event number of finishers":  "event_number_of_finishers",
        "Athlete performance":        "athlete_performance",
        "Athlete club":               "athlete_club",
        "Athlete country":            "athlete_country",
        "Athlete year of birth":      "athlete_year_of_birth",
        "Athlete gender":             "athlete_gender",
        "Athlete age category":       "athlete_age_category",
        "Athlete average speed":      "athlete_average_speed",
        "Athlete ID":                 "source_athlete_id",
    }
    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)

    return df

    df = (
        df.withColumn(
            "event_distance_value",
            F.regxp_extract("event_distance_length", r"([0-9]+\.?[0-9]*)", 1).cast("double"),
        )
        .withColumn(
            "event_distance_unit",
            F.regxp_extract("event_distance_length", r"([a-zA-Z]+)", 1)
        ),
    )

    spark.table("marathon.silver.results_obt") \
        .select("event_distance_length", "event_distance_value", "event_distance_unit") \
        .distinct().show(20, truncate=False)
    
    df = df.withColumn(
        "event_type",
        F.when(F.col("event_distance_unit").isin("km", "mi"), F.lit("distance"))
        .when(F.col("event_distance_unit") == "h", F.lit("timed"))
        .otherwise(F.lit("invaild")),
    )
    spark.table("marathon.silver.results_obt") \
        .groupBy("event_distance_unit", "event_type").count().orderBy("count", ascending=False).show()

    df = df.filter(F.col("event_type") != "invalid")
    df = df.filter(F.col("event_distance_value").isNotNull())

    spark.table("marathon.silver.results_obt").groupBy("event_type").count().show()
        
    perf_time = F.regexp_extract("athlete_performance", r"(\d+):(\d+):(\d+)", 0)
    df = df.withColumn(
        "performance_seconds",
        F.when(
            F.col("event_type") == "distance",
            F.split(perf_time, ":")[0].cast("int") * 3600
            + F.split(perf_time, ":")[1].cast("init") * 60
            + F.split(perf_time, ":")[2].cast("int"),
        ),
    )
    df = df.withColumn(
        "performance_km",
        F.when(
            F.col("event_type") == "timed",
            F.regexp_extract("athlete_performance", r"([0-9]+\.?[0-9]*)", 1).cast("double"),
        ),
    )

    spark.table("marathon.silver.results_obt").select(
        "event_type", "athlete_performance","performance_seconds", "performance_km"
    ).show(25, truncate=False)

    df = (
        df 
        .withColumn("athlete_year_of_birth",
                    F.col("athlete_year_of_birth").cast("double").cast("int"))
        .withColumn("year_of_event", F.col("year_of_event").cast("int"))
        .withColumn("event_number_of_finishers", F.col("event_number_of_finishers").cast("int"))
    )
    df = df.withColumn("athlete_age",
                       F.col("year_of_evemt") - F.col("athlete_year_of_birth"))
    
    df = (
        df
        .withColumn("event_id", F.danse_rank().over(Window.orderBy("event_name")))
        .withColumn("athlete_id", F.danse_rank().over(Window.orderBy("source_athlete_id")))
        .withColumn("result_id", F.monotonically_increasing_id())
        )