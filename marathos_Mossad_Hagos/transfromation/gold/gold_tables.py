from pyspark import pipelines as dp

# reads from silver results_obt_v2 and drops duplicates one row per event
@dp.table(
    name="gold.dim_event",
    comment="One row per event.",
)

# 
def dim_event():
    return (
        spark.read.table("marathos.silver.results_obt_v2")
        .select(
            "event_id",
            "event_name",
            "event_distance_value",
            "event_distance_unit",
            "event_type",
            "event_number_of_finishers",
        )
        .dropDuplicates(["event_id"])
    )

@dp.table(
    name="gold.dim_athlete",
    comment="One row per athlete.",
)
def dim_athlete():
    return (
        spark.read.table("marathos.silver.results_obt_v2")
        .select(
            "athlete_id",
            "source_athlete_id",
            "athlete_gender",
            "athlete_year_of_birth",
            "athlete_club",
            "athlete_country",
            "athlete_age_category",
        )
        .dropDuplicates(["athlete_id"])
    )

@dp.table(
    name="gold.fct_results",
    comment="One row per race result. Keys + measures only.",
)
def fct_results():
    return (
        spark.read.table("marathos.silver.results_obt_v2")
        .select(
            "result_id", 
            "event_id",       
            "athlete_id",     
            "performance_seconds",  
            "performance_km",       
            "athlete_age",          
        )
    )
