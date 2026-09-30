from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.common.config import KAFKA_BOOTSTRAP_SERVERS,CDC_TOPIC
from src.common.schemas import CDC_SCHEMA
from src.common.spark_session import get_spark_session

CURRENT="data/gold/customer_current"

def update_current(batch_df,batch_id):
    if batch_df.rdd.isEmpty(): return
    w=Window.partitionBy("customer_id").orderBy(F.col("event_ts").desc(),F.col("kafka_offset").desc())
    latest=batch_df.withColumn("rn",F.row_number().over(w)).filter("rn=1").drop("rn")
    deletes=latest.filter(F.col("operation")=="DELETE").select("customer_id")
    upserts=latest.filter(F.col("operation")!="DELETE")
    if Path(CURRENT).exists():
        existing=batch_df.sparkSession.read.parquet(CURRENT)
        retained=existing.join(latest.select("customer_id"),"customer_id","left_anti")
        final=retained.unionByName(upserts.select(existing.columns),allowMissingColumns=True)
    else:
        final=upserts
    final.write.mode("overwrite").parquet(CURRENT)

def main():
    spark=get_spark_session("CustomerCDC")
    kafka=(spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS)
           .option("subscribe",CDC_TOPIC).option("startingOffsets","earliest").load())

    raw=kafka.select(F.col("value").cast("string").alias("raw_json"),"topic","partition","offset",
                     F.col("timestamp").alias("kafka_timestamp"))

    bronze=(raw.writeStream.format("parquet").outputMode("append")
            .option("path","data/bronze/customer_cdc")
            .option("checkpointLocation","data/checkpoints/bronze").start())

    parsed=(raw.withColumn("cdc",F.from_json("raw_json",CDC_SCHEMA))
      .select("*","cdc.*").drop("cdc")
      .withColumn("operation",F.when(F.col("op")=="c","INSERT").when(F.col("op")=="u","UPDATE")
                  .when(F.col("op")=="d","DELETE").when(F.col("op")=="r","SNAPSHOT").otherwise("UNKNOWN"))
      .withColumn("record",F.when(F.col("op")=="d",F.col("before")).otherwise(F.col("after")))
      .select("record.*","operation",F.to_timestamp(F.from_unixtime(F.col("ts_ms")/1000)).alias("event_ts"),
              F.col("partition").alias("kafka_partition"),F.col("offset").alias("kafka_offset"),"raw_json"))

    valid=parsed.filter(F.col("customer_id").isNotNull() & F.col("operation").isin("INSERT","UPDATE","DELETE","SNAPSHOT"))
    bad=parsed.filter(F.col("customer_id").isNull() | ~F.col("operation").isin("INSERT","UPDATE","DELETE","SNAPSHOT"))

    qbad=(bad.writeStream.format("parquet").outputMode("append").option("path","data/bad_records")
          .option("checkpointLocation","data/checkpoints/bad").start())

    changes=valid.drop("raw_json")
    qsilver=(changes.writeStream.format("parquet").outputMode("append").option("path","data/silver/customer_changes")
             .option("checkpointLocation","data/checkpoints/silver").start())

    qhistory=(changes.writeStream.format("parquet").outputMode("append").option("path","data/gold/customer_history")
              .option("checkpointLocation","data/checkpoints/history").start())

    qcurrent=(changes.writeStream.foreachBatch(update_current)
              .option("checkpointLocation","data/checkpoints/current").start())

    print("CDC stream running. Ctrl+C to stop.")
    try: spark.streams.awaitAnyTermination()
    finally:
        for q in [bronze,qbad,qsilver,qhistory,qcurrent]:
            if q.isActive:q.stop()
        spark.stop()
if __name__=="__main__": main()
