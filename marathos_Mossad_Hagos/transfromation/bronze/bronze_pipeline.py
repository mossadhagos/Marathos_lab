from pyspark import pipelines as dp

BASE_DIR = "/Volumes/marathos/default/marathos_raw"

@dp.table(
    name="results_raw",
    comment="Raw ultra-marathon race results ingested as-is from the source CSV.",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def results_raw():
    return (
        spark.readStream.format("cloudFiles")          # Auto Loader
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true") # infers schema for you
        .option("header", "true")
        .option("encoding", "UTF-8")
        .load(BASE_DIR)                                # point at the folder
    )