from datetime import datetime
import os
from pyspark.sql.functions import regexp_replace, count, substring
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, when, expr, concat, lit, row_number, collect_list, concat_ws, trim
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

def function_complete_IVR(input_folder, output_folder, partitions, Widget_Process):  

    files = []

    for root, _, file_names in os.walk(input_folder):
        for file_name in file_names:
            if file_name.endswith('.csv') or file_name.endswith('.txt'):
                files.append(os.path.join(root, file_name))

    consolidated_df = None
    for file in files:
        if file.endswith('.csv'):
            df = spark.read.csv(file, header=True, inferSchema=True)
        elif file.endswith('.txt'):
            df = spark.read.option("delimiter", "\t").csv(file, header=True, inferSchema=True)

        if consolidated_df is None:
            consolidated_df = df
        else:
            consolidated_df = consolidated_df.unionByName(df, allowMissingColumns=True)

    if consolidated_df is not None:
        selected_columns = [
            "status", "vendor_lead_code", 
            "source_id", "list_id", "phone_number", "title", "first_name", "last_name"
        ]
        consolidated_df = consolidated_df.select(*selected_columns)

        consolidated_df = consolidated_df.withColumn(
            "status",
            when(col("status") == "AB", "Buzon de voz lleno")
            .when(col("status") == "PM", "Se inicia mensaje y cuelga")                          #Efectivo
            .when(col("status") == "AA", "Maquina contestadora")
            .when(col("status") == "ADC", "Numero fuera de servicio")
            .when(col("status") == "DROP", "No contesta")
            .when(col("status") == "NA", "No contesta")
            .when(col("status") == "NEW", "Contacto sin marcar")
            .when(col("status") == "PDROP", "Error desde el operador")
            .when(col("status") == "PU", "Cuelga llamada")                                      
            .when(col("status") == "SVYEXT", "Llamada transferida al agente")                   #Efectivo
            .when(col("status") == "XFER", "Se reprodujo mensaje completo")                     #Efectivo
            .when(col("status") == "SVYHU", "Cuelga durante una transferencia")                 #Efectivo
            .otherwise("error")
        )

        list_efecty = ["Buzon de voz lleno", "Se inicia mensaje y cuelga", "Maquina contestadora", "Cuelga llamada", "Llamada transferida al agente", "Se reprodujo mensaje completo", "Cuelga durante una transferencia"]

        consolidated_df = consolidated_df.filter(col("status").isin(list_efecty))
        
        consolidated_df = consolidated_df.withColumn(
            "Campana",
            when(col("list_id") == "5401", "GMAC")
            .when(col("list_id") == "4251", "GMAC")
            .when(col("list_id") == "4181", "PASH")
            .when(col("list_id") == "4186", "PASH")
            .otherwise("CLARO")
        )

        consolidated_df = consolidated_df.withColumn(
            "first_name",
            when(col("first_name").isNull() | (trim(col("first_name")) == ""), "0")
            .otherwise(col("first_name"))
        )

        consolidated_df = Function_Modify(consolidated_df)
        partitions = int(partitions)
        output_folder = f"{output_folder}_Consolidado_IVR"

        consolidated_df.repartition(partitions).write.mode('overwrite').csv(output_folder, header=True)

        for root, dirs, files in os.walk(output_folder):
            for file in files:
                if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                    os.remove(os.path.join(root, file))
        
        for i, file in enumerate(os.listdir(output_folder), start=1):
            if file.endswith(".csv"):
                old_file_path = os.path.join(output_folder, file)
                new_file_path = os.path.join(output_folder, f'Consolidado IVR - Part {partitions}.csv')
                os.rename(old_file_path, new_file_path)
        
    else:
        pass
    return consolidated_df

def Function_Modify(RDD):

    Data_Frame = RDD.withColumn("source_id", regexp_replace(col("source_id"), "-", ""))

    Data_Frame = Data_Frame.withColumnRenamed("source_id", "Cuenta_Sin_Punto")
    Data_Frame = Data_Frame.withColumn("Recurso", lit("IVR"))
    Data_Frame = Data_Frame.withColumn("Cuenta_Real", col("Cuenta_Sin_Punto"))
    Data_Frame = Data_Frame.withColumnRenamed("first_name", "Marca")
    Data_Frame = Data_Frame.select("Cuenta_Sin_Punto", "Cuenta_Real", "Marca", "Recurso")
    
    count_df = Data_Frame.groupBy("Cuenta_Real").agg(count("*").alias("Cantidad"))
    
    Data_Frame = Data_Frame.join(count_df, "Cuenta_Real", "left")
    
    Data_Frame = Data_Frame.dropDuplicates(["Cuenta_Real"])

    return Data_Frame