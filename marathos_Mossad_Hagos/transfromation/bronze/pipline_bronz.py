import pyspark as ps 

DATA_PATH = "/Volumes/marathos/default/marathos_raw/TWO_CENTURIES_OF_UM_RACES.csv"

df_marathon = spark.read.csv(f"{DATA_PATH}", header=True)

print(df_marathon)