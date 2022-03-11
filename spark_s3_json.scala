val file = "s3a://slice-data-lake-prod/flink/omp/raw/compressed/order/2022/03/10/16/data-0-222271.gz"
val df = spark.read.json(file)
df.printSchema()
df.select("orderUUID", "merchantName", "orderTotal").show(false)
df.select(from_unixtime((df.col("orderTimestamp")/1000)).cast("timestamp").alias("order_date"), 
          (df.col("orderTotal")/100).alias("order_total"), df.col("merchantName")).show(false)
df.select(df.col("orderUUID"), from_unixtime((df.col("orderTimestamp")/1000)).cast("date").alias("order_date"), 
          (df.col("orderTotal")/100).alias("order_total"), df.col("merchantName")).show(false)

//val file = "s3a://slice-data-lake-prod/flink/omp/raw/compressed/order/2022/03/10/16/data-0-222271.gz"
val file = "s3a://slice-data-lake-prod/flink/omp/raw/compressed/order/2022/03/10/16/*.gz"

spark.sqlContext.sql("CREATE OR REPLACE TEMPORARY VIEW orders USING json OPTIONS" + " (path '" + file + "')")

spark.sqlContext.sql("select count(*) from orders")

spark.sqlContext.sql("""
select orderUUID, mailboxUUID, 
eventType, eventSubType, merchantId, merchantName, 
cast(to_timestamp(orderTimestamp/1000) as date) as order_date, 
size(items) item_count 
from orders 
where size(items) > 1
""").show()

spark.sqlContext.sql("""
with a as (select eventType as event_type, cast(to_timestamp(orderTimestamp/1000) as date) as order_date from orders)
select event_type, count(*) cnt, min(order_date) order_date_min, max(order_date) order_date_max from a group by 1 order by 1
""").show()

//orders.printSchema()
//orders.count()
