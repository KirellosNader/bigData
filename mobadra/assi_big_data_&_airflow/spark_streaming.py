# import findspark
# تهيئة Spark للعمل في بيئة Docker
# findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

# تهيئة Spark Session
# يجب أن نستخدم spark-master كعنوان بدلاً من localhost
spark = SparkSession.builder \
    .appName("GPSDataProcessor") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark Session created successfully.")

# تعريف الـ Schema لبيانات GPS
# يجب أن تتطابق هذه الأنواع مع ما ترسله fake_data.py
gps_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# 1. القراءة من Kafka
kafka_topic = "gps_topic"
kafka_brokers = "kafka:9092" # عنوان Kafka داخل شبكة Docker

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

print(f"Reading from Kafka topic: {kafka_topic}")

# 2. تحليل البيانات (Transformation)
processed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), gps_schema).alias("data")) \
    .select(
        col("data.device_id"),
        col("data.latitude"),
        col("data.longitude"),
        col("data.speed"),
        col("data.timestamp").cast(StringType()).alias("event_timestamp_ms"), # للتحويل إلى صيغة نصية في Postgres
        current_timestamp().alias("processing_time")
    )

# 3. الكتابة إلى Postgres (Sink)
def write_to_postgres(batch_df, batch_id):
    """دالة لكتابة الـ Micro-batch إلى قاعدة بيانات PostgreSQL."""
    try:
        # إذا كانت الدفعة فارغة، لا تفعل شيئًا
        if batch_df.count() == 0:
            return

        # إعدادات Postgres
        jdbc_url = "jdbc:postgresql://postgres:5432/gps_db" # عنوان Postgres داخل شبكة Docker
        db_properties = {
            "user": "user",
            "password": "password",
            "driver": "org.postgresql.Driver",
            "dbtable": "realtime_gps_data" # اسم الجدول
        }

        # يجب إعادة تسمية عمود الـ timestamp لأنه اسم محجوز في Postgres
        final_df = batch_df.withColumnRenamed("event_timestamp_ms", "timestamp_ms")
        
        # الكتابة إلى Postgres باستخدام وضع الإضافة (append)
        final_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", db_properties["dbtable"]) \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", db_properties["driver"]) \
            .mode("append") \
            .save()

        print(f"Batch {batch_id} successfully written to Postgres.")
    
    except Exception as e:
        print(f"Error writing batch {batch_id} to Postgres: {e}")
        # يمكنك إضافة منطق للمحاولة مرة أخرى هنا إذا لزم الأمر

# تشغيل الاستعلام الاسترشادي
query = processed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
