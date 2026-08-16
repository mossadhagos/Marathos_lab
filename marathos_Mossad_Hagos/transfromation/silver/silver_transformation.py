from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Bronze raw data ingestion (auto loader file )
@dp.materialized_view(
    name="silver.results_obt_v2",
    comment="Cleaned one-big-table of ultra-marathon results.",
)

# Reads bronze data and takes the raw csv from bronze layer 

def results_obt_v2():
    df = spark.read.table("marathos.bronze.results_raw")

# Rename columns
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
    # maping columns
    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)

    # Catigorize distance(km/ml) and timed(hours)  
    df = (
        df.withColumn(
            "event_distance_value",
            F.regexp_extract("event_distance_length", r"([0-9]+\.?[0-9]*)", 1).cast("double"),
        )
        .withColumn(
            "event_distance_unit",
            F.regexp_extract("event_distance_length", r"([a-zA-Z]+)", 1)
        )
    )
    
    # Classify event types (distance/timed/invalid)
    df = df.withColumn(
        "event_type",
        F.when(F.col("event_distance_unit").isin("km", "mi"), F.lit("distance"))  
        .when(F.col("event_distance_unit") == "h", F.lit("timed")) 
        .otherwise(F.lit("invalid")), 
    )

    # Filter out bad data 
    df = df.filter(F.col("event_type") != "invalid")
    df = df.filter(F.col("event_distance_value").isNotNull())
        
    # Convert performance times to seconds
    perf_time = F.regexp_extract("athlete_performance", r"(\d+):(\d+):(\d+)", 0)
    df = df.withColumn(
        "performance_seconds",
        F.when(
            F.col("event_type") == "distance",
            F.split(perf_time, ":")[0].cast("int") * 3600
            + F.split(perf_time, ":")[1].cast("int") * 60
            + F.split(perf_time, ":")[2].cast("int"),
        ),
    )
    # ectract numeric value froma performance string and converts it to double 
    df = df.withColumn(
        "performance_km",
        F.when(
            F.col("event_type") == "timed",
            F.regexp_extract("athlete_performance", r"([0-9]+\.?[0-9]*)", 1).cast("double"),
        ),
    )
    # Converting string to int for proper numeric operation 
    df = (
        df 
        .withColumn("year_of_event", F.col("year_of_event").cast("int"))
        .withColumn("event_number_of_finishers", F.col("event_number_of_finishers").cast("int"))
    )
    df = df.withColumn("athlete_age",
                       F.col("year_of_event") - F.col("athlete_year_of_birth").cast("int"))
    
    # creating three surrogate keys 
    df = (
        df
        .withColumn("event_id", F.dense_rank().over(Window.orderBy("event_name")))
        .withColumn("athlete_id", F.dense_rank().over(Window.orderBy("source_athlete_id")))
        .withColumn("result_id", F.monotonically_increasing_id())
    )
    
    return df
