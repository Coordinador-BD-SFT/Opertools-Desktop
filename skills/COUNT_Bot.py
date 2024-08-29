import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, when, expr, concat, lit, row_number, collect_list, concat_ws, trim, count
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

def function_complete_BOT(input_folder, output_folder, partitions, Widget_Process):
    
    spark = SparkSession \
        .builder.appName("Trial") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

    sqlContext = SQLContext(spark)

    files = []
    for root, _, file_names in os.walk(input_folder):
        for file_name in file_names:
            if file_name.endswith('.csv') or file_name.endswith('.csv'):
                files.append(os.path.join(root, file_name))

    consolidated_df = None

    for file in files:
        if file.endswith('.csv'):
            #df = spark.read.csv(file, header=True, inferSchema=True)
            df = spark.read.option("delimiter", ";").csv(file, header=True, inferSchema=True)
        elif file.endswith('.txt'):
            df = spark.read.option("delimiter", "\t").csv(file, header=True, inferSchema=True)

        if consolidated_df is None:
            consolidated_df = df
        else:
            consolidated_df = consolidated_df.unionByName(df, allowMissingColumns=True)

    if consolidated_df is not None:

        consolidated_df = standardize_column_name(consolidated_df)

        selected_columns = [
            "CUENTA", "CUENTA_NEXT", 
            "Edad de Mora"]
        
        consolidated_df = consolidated_df.select(*selected_columns)
        
        consolidated_df = consolidated_df.withColumn(
            "CUENTA",
            when(col("CUENTA").isNull() | (trim(col("CUENTA")) == ""), "0")
            .otherwise(col("CUENTA"))
        )

        partitions = int(partitions)
        output_folder = f"{output_folder}_Consolidado_BOT"

        consolidated_df = Function_Modify(consolidated_df)
        consolidated_df.repartition(partitions).write.mode('overwrite').csv(output_folder, header=True)

        for root, dirs, files in os.walk(output_folder):
            for file in files:
                if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                    os.remove(os.path.join(root, file))
        
        for i, file in enumerate(os.listdir(output_folder), start=1):
            if file.endswith(".csv"):
                old_file_path = os.path.join(output_folder, file)
                new_file_path = os.path.join(output_folder, f'Consolidado BOT - Part {partitions}.csv')
                os.rename(old_file_path, new_file_path)

    else:
        spark.stop()

def Function_Modify(RDD):
    Data_Frame = RDD 
    Data_Frame = Data_Frame.withColumnRenamed("CUENTA", "Cuenta_Sin_Punto")
    Data_Frame = Data_Frame.withColumnRenamed("CUENTA_NEXT", "Cuenta_Real")
    Data_Frame = Data_Frame.withColumn("Recurso", lit("BOT"))
    Data_Frame = Data_Frame.withColumnRenamed("Edad de Mora", "Marca")
    Data_Frame = Data_Frame.select("Cuenta_Sin_Punto", "Cuenta_Real", "Marca", "Recurso")
    
    count_df = Data_Frame.groupBy("Cuenta_Real").agg(count("*").alias("Cantidad"))
    
    Data_Frame = Data_Frame.join(count_df, "Cuenta_Real", "left")
    
    Data_Frame = Data_Frame.dropDuplicates(["Cuenta_Real"])

    return Data_Frame

def standardize_column_name(df):
    
    if "NOMBRE CAMPANA" in df.columns:
        df = df.withColumnRenamed("NOMBRE CAMPANA", "Edad de Mora")

    elif "Edad_Mora" in df.columns:
        df = df.withColumnRenamed("NOMBRE CAMPANA", "Edad de Mora")

    return df