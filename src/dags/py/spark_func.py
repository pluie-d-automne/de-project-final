from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from typing import Dict

spark_jars_packages = ",".join(["org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
                                "com.vertica.jdbc:vertica-jdbc:11.0.2-0"])

def spark_init() -> SparkSession:
    return SparkSession.builder \
                     .appName("Read data from Kafka") \
                     .config("spark.sql.session.timeZone", "UTC") \
                     .config("spark.jars.packages", spark_jars_packages) \
                     .getOrCreate()

def load_df_kafka(spark: SparkSession, topic: StringType, kafka_security_options: Dict) -> DataFrame:
    return spark.readStream \
                .format('kafka') \
                .options(**kafka_security_options) \
                .option('subscribe', topic) \
                .load()

def transform(df: DataFrame) -> DataFrame:
    schema = StructType([StructField("object_id", StringType()),
                    StructField("object_type", StringType()),
                    StructField("sent_dttm", StringType()),
                    StructField("payload", StringType())])
    df = df.withColumn("value", df.value.cast("string"))\
        .withColumn("key", df.key.cast("string"))
    df = df.withColumn("values",f.from_json(f.col("value"), schema))\
        .selectExpr(["values.object_id as object_id",
                         "values.object_type as object_type",
                         "values.sent_dttm as sent_dttm",
                         "values.payload as payload"
                ])
    return df

def create_foreach_batch_function(vertica_url, vertica_settings):
    def foreach_batch_function(df, epoch_id):
        # Сохраним значение, чтобы не пересчитывать
        p_df = df.persist()

        # Вытащим строки, относящиеся к currencies, преобразуем их и запишем в Vertica
        currency_schema = StructType([StructField("date_update", StringType()),
                    StructField("currency_code", IntegerType()),
                    StructField("currency_code_with", IntegerType()),
                    StructField("currency_with_div", DecimalType(14,2))])
    
        currency_df = p_df[p_df["object_type"]=='CURRENCY']
    
        if currency_df.count()>0:
        
            currency_df = currency_df.withColumn("payload",f.from_json(f.col("payload"), currency_schema))\
                .selectExpr(["payload.date_update as date_update",
                         "payload.currency_code as currency_code",
                         "payload.currency_code_with as currency_code_with",
                         "payload.currency_with_div as currency_with_div"
                        ])
        
            currency_df.write\
            .format('jdbc').options(
                url=vertica_url,
                driver=vertica_settings['driver'],
                dbtable="PDKUDRYAVTSEVAYANDEXRU__STAGING.currencies",
                user=vertica_settings['user'],
                password=vertica_settings['password'])\
                .mode('append').save()
    
            
        # Вытащим строки, относящиеся к transactions, преобразуем их и запишем в Vertica
        transaction_schema = StructType([StructField("operation_id", StringType()),
                        StructField("account_number_from", IntegerType()),
                        StructField("account_number_to", IntegerType()),
                        StructField("currency_code", IntegerType()),
                        StructField("country", StringType()),
                        StructField("status", StringType()),
                        StructField("transaction_type", StringType()),
                        StructField("amount", DecimalType(14,2)),
                        StructField("transaction_dt", StringType())])        
        
        transaction_df = p_df[p_df["object_type"]=='TRANSACTION']
        
        if transaction_df.count()>0:
        
            transaction_df = transaction_df.withColumn("payload",f.from_json(f.col("payload"), transaction_schema))\
            .selectExpr(["payload.operation_id as operation_id",
                            "payload.account_number_from as account_number_from",
                            "payload.account_number_to as account_number_to",
                            "payload.currency_code as currency_code",
                            "payload.country as country",
                            "payload.status as status",
                            "payload.transaction_type as transaction_type",
                            "payload.amount as amount",
                            "payload.transaction_dt as transaction_dt"
                    ])
        
            transaction_df.write\
                .format('jdbc').options(
                url=vertica_url,
                driver=vertica_settings['driver'],
                dbtable="PDKUDRYAVTSEVAYANDEXRU__STAGING.transactions",
                user=vertica_settings['user'],
                password=vertica_settings['password'])\
                .mode('append').save()
        # Удалим ранее сохранённый датафрейм    
        p_df.unpersist()  
    return foreach_batch_function               
            
