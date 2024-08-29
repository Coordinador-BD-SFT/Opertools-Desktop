from datetime import datetime
import os
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, when, expr, concat, lit, row_number, collect_list, concat_ws, trim, count
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

def function_complete_DEMO(input_folder, output_folder, Partitions):

    files = []

    for root, _, file_names in os.walk(input_folder):
        for file_name in file_names:
            if file_name.endswith('.csv') or file_name.endswith('.txt'):
                files.append(os.path.join(root, file_name))

    consolidated_df = None
    for file in files:
        if file.endswith('.csv'):
            df = spark.read.option("delimiter", ";").csv(file, header=True, inferSchema=True)
        elif file.endswith('.txt'):
            df = spark.read.option("delimiter", "\t").csv(file, header=True, inferSchema=True)

        if consolidated_df is None:
            consolidated_df = df
        else:
            consolidated_df = consolidated_df.unionByName(df, allowMissingColumns=True)

    if consolidated_df is not None:
        
        selected_columns = [
            "tipodato", "identificacion", "cuenta"
        ]

        consolidated_df = consolidated_df.select(*selected_columns)

        consolidated_df = consolidated_df.withColumn(
            "phone_type",
            when((col("tipodato") >= 300000000) & (col("tipodato") <= 3599999999), "Celular")
            .otherwise("Fijo")
        )

        consolidated_df = consolidated_df.filter(col("phone_type") == "Celular")

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")
        output_folder_ = f"{output_folder}/Consolidado_DEMOGRÁFICOS_{Time_File}"

        consolidated_df = Function_ADD(consolidated_df)
        
        Partitions = int(Partitions)
        consolidated_df.repartition(Partitions).write.mode("overwrite").option("header", "true").csv(output_folder_)
            
        for root, dirs, files in os.walk(output_folder_):
            for file in files:
                if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                    os.remove(os.path.join(root, file))
        
        for i, file in enumerate(os.listdir(output_folder_), start=1):
            if file.endswith(".csv"):
                old_file_path = os.path.join(output_folder_, file)
                new_file_path = os.path.join(output_folder_, f'Demograficos {Time_File} Part- {i}.csv')
                os.rename(old_file_path, new_file_path)
    else:
        pass
    return consolidated_df

def Function_ADD(RDD):

    RDD = RDD.withColumn("Cruice", concat(col("tipodato"), col("cuenta")))
    RDD = RDD.dropDuplicates(["Cruice"])

    RDD_Trial = RDD.groupBy("tipodato").agg(count("*").alias("Cantidad"))
    RDD = RDD.join(RDD_Trial, "tipodato", "left")

    RDD = RDD.withColumnRenamed("identificacion", "Documento")
    RDD = RDD.withColumnRenamed("cuenta", "Cuenta")
    RDD = RDD.withColumnRenamed("tipodato", "Dato_Linea")
    
    RDD = RDD.withColumn("Cuenta_Next",concat( col("Cuenta"), lit("-")))
    RDD = RDD.select(col("Dato_Linea"), col("Documento"), col("Cuenta"), col("Cuenta_Next"))
                         
    return RDD