import os
import modules.Filter_Base as Filter_Base
import pyspark
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number
from pyspark.sql.functions import expr, when, to_date, datediff, current_date, split, length

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)


### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, partitions, filter_brands, filter_origins, Dates, today_IVR, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = change_name_column(Data_Frame, "nombrecompleto")
    Data_Frame = IVR_Process(Data_Frame, output_directory, partitions, filter_brands, filter_origins, \
                             Dates, today_IVR, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter)


### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True,sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    return DF

def change_name_column (Data_, Column):

    Data_ = Data_.withColumn(Column, upper(col(Column)))

    character_list_N = ["\\ÃƒÂ‘", "\\Ã‚Â¦", "\\Ã‘", "Ñ", "ÃƒÂ‘", "Ã‚Â¦", "Ã‘"]
    
    for character in character_list_N:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, "NNNNN"))
    
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "NNNNN", "N"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "Ã‡", "A"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "ÃƒÂ", "I"))


    character_list = ["SR/SRA", "SR./SRA.", "SR/SRA.","SR.", "SRA.", "SR(A).","SR ", "SRA ", "SR(A)",\
                    "\\.",'#', '$', '/','<', '>', "\\*", "SEÑORES ","SEÑOR(A) ","SEÑOR ","SEÑORA ", "SENORES ",\
                    "SENOR(A) ","SENOR ","SENORA ", "¡", "!", "\\?" "¿", "_", "-", "}", "\\{", "\\+", "0 ", "1 ", "2 ", "3 ",\
                     "4 ", "5 ", "6 ", "7 ","8 ", "9 ", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "  "]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))

    
    return Data_

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("fechagestion_contactodirecto", "FECHA_CONTACTO")
    Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "NOMBRE")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_asignacion", "FECHA_ASIGNACION")
    Data_Frame = Data_Frame.withColumnRenamed("marca", "FIRST NAME")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_vencimiento", "LAST NAME")
    Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "MONTO_INICIAL")
    Data_Frame = Data_Frame.withColumnRenamed("descuento", "DESCUENTO")
    Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "PHONE NUMBER")
    Data_Frame = Data_Frame.withColumnRenamed("identificacion", "VENDOR LEAD CODE")
    Data_Frame = Data_Frame.withColumnRenamed("origen", "TITLE")
    Data_Frame = Data_Frame.withColumnRenamed("cuenta", "SOURCE ID")
    Data_Frame = Data_Frame.withColumnRenamed("dias_transcurridos", "DIAS DE MORA")

    Data_Frame = Data_Frame.select("VENDOR LEAD CODE", "SOURCE ID", "PHONE NUMBER", "TITLE", "FIRST NAME", "**", "LAST NAME", \
                         "FECHA_ASIGNACION", "FECHA_CONTACTO", "MONTO_INICIAL", "marca2", "DESCUENTO", "tipo_pago","DIAS DE MORA", \
                            "NOMBRE", "Tipo de Linea")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, partitions, widget_filter):

    if widget_filter != "Tables":

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")
        File_Date = now.strftime("%Y%m%d")
        Type_File = "IVR_"
        
        output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
        Data_Frame.repartition(partitions).write.mode("overwrite").option("header", "true").csv(output_path)

        for root, dirs, files in os.walk(output_path):
            for file in files:
                if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                    os.remove(os.path.join(root, file))
        
        for i, file in enumerate(os.listdir(output_path), start=1):
            if file.endswith(".csv"):
                old_file_path = os.path.join(output_path, file)
                new_file_path = os.path.join(output_path, f'IVR {File_Date} Part- {i}.csv')
                os.rename(old_file_path, new_file_path)

    else:
        Data_Frame = Data_Frame
        
    return Data_Frame

### Dinamización de columnas de contacto
def Phone_Data(Data_):

    columns_to_stack_min = ["min"]
    columns_to_stack_celular = [f"celular{i}" for i in range(1, 11)]
    columns_to_stack_fijo = [f"fijo{i}" for i in range(1, 4)]
    all_columns_to_stack = columns_to_stack_celular + columns_to_stack_fijo + columns_to_stack_min
    columns_to_drop_contact = all_columns_to_stack
    stacked_contact_data_frame = Data_.select("*", *all_columns_to_stack)

    stacked_contact_data_frame = stacked_contact_data_frame.select(
        "*",
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as Dato_Contacto")
    )
    Data_ = stacked_contact_data_frame.drop(*columns_to_drop_contact)

    return Data_

### Proceso de filtrado de líneas
def IVR_Process (Data_, Directory_to_Save, partitions, filter_brands, filter_origins, Dates, today_IVR, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter):

    Data_ = Filter_Base.Function_Complete(Data_)
    
    filter_cash = ["", "Pago Parcial", "Sin Pago"]
    Data_ = Data_.filter((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""))

    Data_ = Data_.withColumn(
        "tipo_pago", 
        when(((col("tipo_pago").isNull()) | (col("tipo_pago") == "")), lit("Sin Pago"))
        .otherwise(col("tipo_pago")))
    
    Data_ = Data_.withColumn(
        "fechagestion_contactodirecto", 
        when((col("fechagestion_contactodirecto").isNull() | (col("fechagestion_contactodirecto") == "")), lit("2000-04-30"))
        .otherwise(col("fechagestion_contactodirecto")))
    
    Data_ = Data_.filter(col("marca").isin(filter_brands))
    Data_ = Data_.filter(col("origen").isin(filter_origins))

    Data_ = Function_Filter(Data_, Dates, today_IVR, Benefits, Contacts_Min, Value_Min, Value_Max)
    
    Data_ = Data_.withColumn("Telefono 2", lit(""))
    Data_ = Data_.withColumn("Telefono 3", lit(""))
    Data_ = Data_.withColumn("**2", lit(""))
    Data_ = Data_.withColumn("**", lit(""))

    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))

    Data_ = Data_.withColumn(
        "Mod_init_cta", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    Data_ = Data_.withColumn(
        "descuento", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), lit("0"))
        .otherwise(col("descuento")))

    Data_ = Data_.withColumn("Mod_init_cta", col("Mod_init_cta").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.select("Dato_Contacto", "Telefono 2", "Telefono 3", "**", "identificacion", "origen", "**2", "cuenta", \
                         "marca", "fecha_vencimiento", "fecha_asignacion", "fechagestion_contactodirecto", "Mod_init_cta", \
                         "marca2", "descuento", "tipo_pago", "nombrecompleto")
    
    Data_ = Data_.dropDuplicates(["Dato_Contacto"])
    Order_Columns = ["Mod_init_cta", "origen","Dato_Contacto",'marca', "fechagestion_contactodirecto"]

    for Column in Order_Columns:
        Data_ = Data_.orderBy(col(Column).desc())

    Data_ = Data_.withColumn("now", current_date())
    Data_ = Data_.withColumn("dias_transcurridos", datediff(col("now"), col("fecha_vencimiento")))

    Data_ = Data_.withColumn("nombrecompleto", split(col("nombrecompleto"), " "))

    for position in range(4):
        Data_ = Data_.withColumn(f"Name_{position}", (Data_["nombrecompleto"][position]))

    Data_ = Data_.withColumn("nombrecompleto", when(length(col("Name_1")) > 3, col("Name_1"))
                             .when(length(col("Name_2")) > 3, col("Name_2"))
                             .when(length(col("Name_0")) > 3, col("Name_0"))
                             .when(length(col("Name_3")) > 3, col("Name_3"))
                             .otherwise(col("Name_1")))

    Data_ = Data_.withColumn("Tipo de Linea", when(col("Dato_Contacto") < 6000000000, lit("Celular"))
                .otherwise(lit("Fijo")))
    
    Data_ = Renamed_Column(Data_)
    Save_Data_Frame(Data_, Directory_to_Save, partitions, widget_filter)
    
    return Data_

def Function_Filter(RDD, Dates, today_IVR, Benefits, Contacts_Min, Value_Min, Value_Max):
    
    RDD = RDD.filter(col("fechagestion_contactodirecto") != today_IVR)
    
    if Dates == "All Dates":
        pass
    else: 
        RDD = RDD.filter(col("fecha_vencimiento") == Dates)

    RDD = RDD.withColumn("Referencia",  when(col("origen") == "RR", col("cuenta")).otherwise(col("Referencia")))           
    RDD = RDD.filter(col("Referencia") != "")

    if Contacts_Min == "Celular":
        Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
        Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
        RDD = Data_C

    elif Contacts_Min == "Fijo":
        Data_F = RDD.filter(col("Dato_Contacto") >= 6010000009)
        Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
        RDD = Data_F
    
    else:
        Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
        Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
        Data_F = RDD.filter(col("Dato_Contacto") >= 6010000009)
        Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
        RDD = Data_C.union(Data_F)
    
    RDD = RDD.filter(col("Mod_init_cta") >= Value_Min)
    RDD = RDD.filter(col("Mod_init_cta") <= Value_Max)

    RDD = RDD.withColumn(
        "DTO_Filter", 
        when((col("descuento") == "0%") | (col("descuento") == "0") | (col("descuento").isNull()) | (col("descuento") == "N/A"), lit("Sin Descuento"))
        .otherwise(lit("Con Descuento")))
    
    if Benefits == "Con Descuento":
        RDD = RDD.filter(col("DTO_Filter") == "Con Descuento")

    elif Benefits == "Sin Descuento":
        RDD = RDD.filter(col("DTO_Filter") == "Sin Descuento")

    else:
        RDD = RDD

    return RDD