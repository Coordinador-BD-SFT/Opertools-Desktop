import os
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, length, split, to_date
from pyspark.sql.functions import trim, format_number, expr, when, coalesce, datediff, current_date

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

def Function_Complete(Data_):

    #### Change value of DTO
    Data_ = Data_.withColumn(
        "descuento",
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A") | (col("descuento") == "Sin Descuento"), lit("0"))
        .otherwise(col("descuento")))
    
    #### Inclusion of brand (dont exist)
    Data_ = Data_.withColumn(
        "marca", 
        when(((col("marca_refinanciado") == "REFINANCIADO")) , lit("potencial a castigar"))
        .otherwise(col("marca")))
    
    #### Type of Transaction
    Data_ = Data_.withColumn(
        "tipo_pago", 
        when(((col("tipo_pago").isNull()) | (col("tipo_pago") == "")), lit("Sin Pago"))
        .otherwise(col("tipo_pago")))
    
    #### Change Brand for Apple
    Data_ = Data_.withColumn(
        "marca2",
        when(col("ciudad") == "ASIGNACION_MANUAL_APPLE", lit("Apple Manual"))
        .otherwise(col("marca")))

    return Data_