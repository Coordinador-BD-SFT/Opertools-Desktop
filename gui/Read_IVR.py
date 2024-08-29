from datetime import datetime
import os
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, when, expr, concat, lit, row_number, collect_list, concat_ws, trim
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

def function_complete_IVR(input_folder, output_folder, Partitions, Widget_Process, Date, Brands, Channel):

    files = []

    for root, _, file_names in os.walk(input_folder):
        for file_name in file_names:
            if file_name.endswith('.csv') or file_name.endswith('.txt'):
                print(f"{_}")
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
            "last_local_call_time", "status", "vendor_lead_code", 
            "source_id", "list_id", "phone_number", "title", "first_name", "last_name"
        ]

        consolidated_df = consolidated_df.select(*selected_columns)

        consolidated_df = consolidated_df.withColumn("last_local_call_time", regexp_replace(col("last_local_call_time"), "T", " "))
        consolidated_df = consolidated_df.withColumn("last_local_call_time", regexp_replace(col("last_local_call_time"), ".000-05:00", ""))

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

        consolidated_df = consolidated_df.withColumn(
            "title",
            when(col("title") == "ASCA", "Equipo")
            .when(col("title") == "RR", "Hogar")
            .when(col("title") == "SGA", "Negocios")
            .when(col("title") == "BSCS", "Postpago")
            .otherwise("error")
        )
        
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
            when((col("first_name").isNull() | (trim(col("first_name")) == ""))\
                  & (col("title").isNull() | (trim(col("title")) == ""))\
                      & (col("last_name").isNull() | (trim(col("last_name")) == "")), "GMAC")
            .otherwise(col("first_name")))

        consolidated_df = consolidated_df.withColumn(
            "first_name",
            when(col("first_name").isNull() | (trim(col("first_name")) == ""), "0")
            .otherwise(col("first_name"))
        )

        Transaccional = ((col("list_id") == "4000") | (col("list_id") == "4041") | (col("list_id") == "4061")\
                         | (col("list_id") == "4081") | (col("list_id") == "4101") | (col("list_id") == "4121")\
                            | (col("list_id") == "4141") | (col("list_id") == "4161") | (col("list_id") == "4181")\
                                | (col("list_id") == "4186") | (col("list_id") == "4191") | (col("list_id") == "4196")\
                                    | (col("list_id") == "4251") | (col("list_id") == "5421") | (col("list_id") == "5441"))

        consolidated_df = consolidated_df.withColumn("Canal", when(Transaccional, "Transacccional")\
                                        .otherwise(lit("IVR Intercom")))

        if Date == "All Dates":
            pass
        else:
            consolidated_df = consolidated_df.filter(col("last_local_call_time") == Date)

        if Brands == "Todo":
            pass
        else:
            consolidated_df = consolidated_df.filter(col("first_name") == str(Brands))

        if Channel == "Todo":
            pass
        elif Channel == "Transaccionales":
            consolidated_df = consolidated_df.filter(col("Canal") == "Transacccional")
        else:
            consolidated_df = consolidated_df.filter(col("Canal") == "IVR Intercom")

        consolidated_df = consolidated_df.withColumn(
            "phone_type",
            when((col("phone_number").cast("long") >= 300000000) & (col("phone_number").cast("long") <= 3599999999), "Celular")
            .otherwise("Fijo")
        )

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")
        output_folder_ = f"{output_folder}/Consolidado_IVR_{Time_File}"

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
                new_file_path = os.path.join(output_folder_, f'Consolidado IVR Part- {i}.csv')
                os.rename(old_file_path, new_file_path)

        #Effectiveness_df(consolidated_df, output_folder2_)
    else:
        pass
    return consolidated_df

def Function_ADD(RDD):

    filter_origins = ["Postpago", "Equipo", "Hogar", "Negocios", "Portofino", "Atmos", "Seven-Seven", "Patprimo"]
    #RDD = RDD.filter(col("title").isin(filter_origins))

    RDD = RDD.withColumn(
            "effectiveness",
            when((col("status") == "Se inicia mensaje y cuelga") | (col("status") == "Llamada transferida al agente")\
                 | (col("status") == "Se reprodujo mensaje completo") | (col("status") == "Cuelga durante una transferencia"), "Efectivo")

            .otherwise("No efectivo")
        )

    RDD = RDD.withColumnRenamed("last_local_call_time", "Ultima Marcacion")
    RDD = RDD.withColumnRenamed("status", "Estado de Llamada")
    RDD = RDD.withColumnRenamed("vendor_lead_code", "Documento")
    RDD = RDD.withColumnRenamed("source_id", "Cuenta")
    RDD = RDD.withColumnRenamed("list_id", "Lista - Canal")
    RDD = RDD.withColumnRenamed("phone_number", "Linea")
    RDD = RDD.withColumnRenamed("title", "Origen")
    RDD = RDD.withColumnRenamed("first_name", "Marca")
    RDD = RDD.withColumnRenamed("phone_type", "Tipo de Linea")
    RDD = RDD.withColumnRenamed("effectiveness", "Efectividad")
    RDD = RDD.withColumnRenamed("last_name", "FLP")

    RDD = RDD.withColumn(
            "Origen",
            when((col("Campana") == "PASH"), col("Cuenta"))
            .otherwise(col("Origen"))
        )
    RDD = RDD.withColumn(
            "Cuenta",
            when((col("Campana") == "PASH"), concat(col("Documento"), lit("_"), col("Marca"), lit("_"), col("Cuenta")))
            .otherwise(col("Cuenta"))
        )

    return RDD

def Effectiveness_df(RDD, output_folder2):

    filter_effectiveness = ["Efectivo", "Contestada Parcial"]
    RDD = RDD.filter(col("Efectividad").isin(filter_effectiveness))
    RDD = RDD.withColumn("filter", concat(col("Cuenta"), lit("-"), col("Linea")))
    RDD = RDD.dropDuplicates(["filter"])

    windowSpec = Window.partitionBy("filter").orderBy("Cuenta","Linea")
    RDD = RDD.withColumn("phone_row_num", row_number().over(windowSpec))
    RDD = RDD.filter(col("phone_row_num") <= 30)

    for i in range(1, 31):
        RDD = RDD.withColumn(f"phone_{i}_temp", when(col("phone_row_num") == i, col("phone_number")).otherwise(lit("")))

    RDD_consolidated = RDD.groupBy("Cuenta").agg(*[concat_ws(";", collect_list(col(f"phone_{i}_temp"))).alias(f"phone_{i}") for i in range(1, 31)])


    RDD = RDD.select("Cuenta", "Documento", "Origen", "Marca")
    RDD = RDD.join(RDD_consolidated, "Cuenta", "left")
    
    for col_name, data_type in RDD.dtypes:
        if data_type == "double":
           RDD = RDD.withColumn(col_name, col(col_name).cast(StringType()))
    
    all_columns = RDD.columns

    columns_to_pass = [col_name for col_name in all_columns if col_name != "Linea"]
    columns_to_pass = [col_name for col_name in all_columns if col_name != "phone_row_num"]
    RDD = RDD.select(*columns_to_pass)

    RDD.printSchema()
    RDD.repartition(1).write.mode("overwrite").option("header", "true").csv(f"{output_folder2}_")

    return RDD