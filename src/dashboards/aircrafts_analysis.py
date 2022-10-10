import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (SparkSession.builder
         .appName("Aircrafts Analysis")
         .getOrCreate()
         )

df1 = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "localhost:29092")
       .option("subscribe", "airtraffic")
       .option("startingOffsets", "earliest")
       .load()
       )

df2 = df1.selectExpr("CAST(value AS STRING)")


aircraft = {
    "aircraft_name": "",
    "from": "",
    "to": "",
    "passengers": 0
}

schema = F.schema_of_json(F.lit(json.dumps(aircraft)))

airtraffic = (df2.select(F.from_json(df2.value, schema).alias("jsondata"))
              .select("jsondata.*")
              )

aircrafts = (airtraffic.groupBy("aircraft_name")
             .agg({"passengers": "sum"})
             .withColumnRenamed("sum(passengers)", "total_passengers")
             .orderBy("total_passengers", ascending=False)
             )


(aircrafts.writeStream
 .format("console")
 .outputMode("complete")
 .start()
 .awaitTermination()
 )
