import os
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
def Function_Complete(path, output_directory, Partitions, Type_Range, Type_Client, Contacts_Min, Value_Min, Value_Max):
    
    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = change_name_column(Data_Frame, "nombrecompleto")
    Data_Frame = IVR_Process(Data_Frame, output_directory, Partitions, Type_Range, Type_Client, \
                              Contacts_Min, Value_Min, Value_Max)
    
    return Data_Frame


### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True, sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
    DF = change_character_account(DF, "cuenta")
    DF = change_name_column(DF, "nombrecompleto")

    return DF

### Limpieza de carácteres especiales en la columna de cuenta
def change_character_account (Data_, Column):

    character_list = ["-"]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), \
        character, ""))

    return Data_

### Limpieza de nombres
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

    Data_Frame = Data_Frame.withColumnRenamed("fechagestion", "FECHA_CONTACTO")
    Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "NOMBRE")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_ingreso", "FECHA_ASIGNACION")
    Data_Frame = Data_Frame.withColumnRenamed("tipo_de_cliente", "FIRST NAME")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_castigo", "LAST NAME")
    Data_Frame = Data_Frame.withColumnRenamed("deuda_total", "MONTO_INICIAL")
    Data_Frame = Data_Frame.withColumnRenamed("estado", "ESTADO")
    Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "PHONE NUMBER")
    Data_Frame = Data_Frame.withColumnRenamed("identificacion", "VENDOR LEAD CODE")
    Data_Frame = Data_Frame.withColumnRenamed("rango", "TITLE")
    Data_Frame = Data_Frame.withColumnRenamed("cuenta", "SOURCE ID")
    Data_Frame = Data_Frame.withColumnRenamed("dias_de_mora", "DIAS DE MORA")

    Data_Frame = Data_Frame.select("VENDOR LEAD CODE", "SOURCE ID", "PHONE NUMBER", "TITLE", "FIRST NAME", "**", "LAST NAME", \
                         "FECHA_ASIGNACION", "FECHA_CONTACTO", "MONTO_INICIAL", "DEUDA_REAL", "ESTADO", "DIAS DE MORA", \
                            "NOMBRE", "Honorarios GM", "Descuento Rango", "Tipo de Linea")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"Sistema_IVR_GM_"

    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").csv(output_path)
    
    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                os.remove(os.path.join(root, file))
    
    for i, file in enumerate(os.listdir(output_path), start=1):
        if file.endswith(".csv"):
            old_file_path = os.path.join(output_path, file)
            new_file_path = os.path.join(output_path, f'IVR Part- {i}.csv')
            os.rename(old_file_path, new_file_path)

    return Data_Frame

### Dinamización de columnas de celulares
def Phone_Data(Data_):

    columns_to_stack = [f"celular{i}" for i in range(1, 11)]
    columns_aditional = ["celular", "tel_casa_codeudor", "tel_oficina_codeudor", "celular_codeudor", "telefono_casa"]
    columns_to_drop = columns_to_stack + columns_aditional
    Stacked_Data_Frame = Data_.select("*", *columns_to_stack)
    
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(columns_to_stack)}, {', '.join(columns_to_stack)}) as Dato_Contacto")
        )
    
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop)
    Stacked_Data_Frame = Data_.select("*")

    return Stacked_Data_Frame

### Proceso de filtrado de líneas
def IVR_Process (Data_, Directory_to_Save, partitions, Type_Range, Type_Client, Contacts_Min, Value_Min, Value_Max):

    Data_ = Data_.withColumn(
        "tipo_de_cliente", 
        when(col("tipo_de_cliente").isNull(), lit("Nulo"))
        .otherwise(col("tipo_de_cliente")))
    
    Data_ = Data_.withColumn(
        "rango", 
        when(col("rango").isNull(), lit("NULO"))
        .otherwise(col("rango")))

    
    Data_ = Data_.filter(col("rango").isin(Type_Range))
    Data_ = Data_.filter(col("tipo_de_cliente").isin(Type_Client))

    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))

    Price_Col = "deuda_total"     

    Data_ = Data_.withColumn(f"DEUDA_REAL", col(f"{Price_Col}").cast("double").cast("int"))

    Data_ = Data_.withColumn(
        "Honorarios GM", 
        when((col("tipo_de_cliente") == "Integral"), lit("1428")) \
        .when((col("tipo_de_cliente") == "Comercial"), lit("123")) \
        .otherwise(lit("0")))
    
    Data_ = Data_.withColumn(
        "Descuento Rango", 
        when((col("rango") == "SALDO INSOLUTO"), lit(65)) \
        .when((col("rango") == "R1"), lit(90)) \
        .when((col("rango") == "R2"), lit(80)) \
        .when((col("rango") == "R3"), lit(45)) \
        .when((col("rango") == "R4"), lit(28)) \
        .otherwise(lit(100)))
    
    Data_ = Data_.withColumn("deuda_total",(col("deuda_total") + ((col("deuda_total") * (col("Honorarios GM")/100)) / 100)))
    Data_ = Data_.withColumn("deuda_total",((col("deuda_total") * col("Descuento Rango")) / 100))

    Data_ = Function_Filter(Data_, Contacts_Min, Value_Min, Value_Max)
    
    Data_ = Data_.withColumn("Telefono 2", lit(""))
    Data_ = Data_.withColumn("Telefono 3", lit(""))
    Data_ = Data_.withColumn("**2", lit(""))
    Data_ = Data_.withColumn("**", lit(""))

    Data_ = Data_.withColumn("Honorarios GM", col("Honorarios GM") / 100)
                             
    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))

    Data_ = Data_.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    value_car = 10000000 ##10 MILLONES|

    Data_ = Data_.withColumn("Rango_Deuda", \
            when((Data_.deuda_total <= value_car * 1), lit("1 Menos a 10 millones")) \
                .when((Data_.deuda_total <= value_car * 2), lit("2 Entre 10 a 20 millones")) \
                .when((Data_.deuda_total <= value_car * 3), lit("3 Entre 20 a 30 millones")) \
                .when((Data_.deuda_total <= value_car * 4), lit("4 Entre 30 a 40 millones")) \
                .when((Data_.deuda_total <= value_car * 5), lit("5 Entre 40 a 50 millones")) \
                .when((Data_.deuda_total <= value_car * 6), lit("6 Entre 50 a 60 millones")) \
                .when((Data_.deuda_total <= value_car * 7), lit("7 Entre 60 a 70 millones")) \
                .when((Data_.deuda_total <= value_car * 8), lit("8 Entre 70 a 80 millones")) \
                .when((Data_.deuda_total <= value_car * 9), lit("9 Entre 80 a 90 millones")) \
                .otherwise(lit("9.1 Mayor a 90 millones")))
    
    Data_ = Data_.dropDuplicates(["Cruce_Cuentas"])

    Data_ = Data_.withColumn(
        "etapa_procesal", 
        when(col("etapa_procesal").isNull(), lit("NULO"))
        .otherwise(col("etapa_procesal")))

    Data_ = Data_.select("Dato_Contacto", "Telefono 2", "Telefono 3", "**", "identificacion", "rango", "**2", "cuenta", \
                         "tipo_de_cliente", "fecha_castigo", "fecha_ingreso", "fechagestion", f"{Price_Col}", \
                         "dias_de_mora", "estado", "nombrecompleto", "DEUDA_REAL", "Honorarios GM", "Descuento Rango")
    
    Data_ = Data_.dropDuplicates(["Dato_Contacto"])
    Order_Columns = [f"{Price_Col}", "rango","Dato_Contacto",'tipo_de_cliente', "fechagestion"]

    for Column in Order_Columns:
        Data_ = Data_.orderBy(col(Column).desc())

    Data_ = Data_.withColumn("nombrecompleto", split(col("nombrecompleto"), " "))

    for position in range(4):
        Data_ = Data_.withColumn(f"Name_{position}", (Data_["nombrecompleto"][position]))

    Data_ = Data_.withColumn("nombrecompleto", when(length(col("Name_1")) > 3, col("Name_1"))
                             .when(length(col("Name_2")) > 3, col("Name_2"))
                             .when(length(col("Name_0")) > 3, col("Name_0"))
                             .when(length(col("Name_3")) > 3, col("Name_3"))
                             .otherwise(col("Name_1")))

    Data_ = Data_.withColumn("Tipo de Linea", when(col("Dato_Contacto") < 6010000000, lit("Celular"))
                .otherwise(lit("Fijo")))
    
    Data_ = Renamed_Column(Data_)
    Save_Data_Frame(Data_, Directory_to_Save, partitions)
    
    return Data_

def Function_Filter(RDD, Contacts_Min, Value_Min, Value_Max): 

    RDD = RDD.filter(col("mejorperfil_mes") != "Acuerdo de Pago Cuotas")
    RDD = RDD.filter(col("mejorperfil_mes") != "Acuerdo de Pago Total")
    RDD = RDD.filter(col("mejorperfil_mes") != "Contacto Titular con PP")
    RDD = RDD.filter(col("mejorperfil_mes") != "Ya Pago")
    RDD = RDD.filter(col("mejorperfil_mes") != "Numero Errado")
    RDD = RDD.filter(col("mejorperfil_mes") != "Dacion en Tramite")
    RDD = RDD.filter(col("mejorperfil_mes") != "Dacion Legalizado")
    #### PERFIL

    RDD = RDD.filter(col("proceso_actual") != "CAPTURADO")
    RDD = RDD.filter(col("proceso_actual") != "RECLAMACION SEGURO")
    RDD = RDD.filter(col("proceso_actual") != "LIQUIDACION PATRIMONIAL")
    RDD = RDD.filter(col("proceso_actual") != "DACION")
    RDD = RDD.filter(col("proceso_actual") != "PROCESO SUSPENDIDO")
    RDD = RDD.filter(col("proceso_actual") != "RECLAMACION SEGURO")
    RDD = RDD.filter(col("proceso_actual") != "PROCESO EN REORGANIZACION")
    #### PROCESS

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
    
    RDD = RDD.filter(col("deuda_total") >= Value_Min)
    RDD = RDD.filter(col("deuda_total") <= Value_Max)

    return RDD

File_Name = "reporte_clientes_GM"
path = f'C:/Users/c.operativo/Downloads/{File_Name}.csv'
output_directory = 'C:/Users/c.operativo/Downloads/'

Partitions = 1
Type_Range = ["R3","R4","SALDO INSOLUTO","R1","R2"]
Type_Range = ["R3","R4"]
Type_Client = ["Comercial", "Integral", "Nulo"]
Contacts_Min = "tODOS"

Value_Min = 0
Value_Max = 99999999999999

Function_Complete(path, output_directory, Partitions, Type_Range, Type_Client, Contacts_Min, Value_Min, Value_Max)