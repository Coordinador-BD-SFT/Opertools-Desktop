from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, concat_ws
from pyspark.sql.functions import expr, when, row_number, collect_list, length
import os

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, Partitions, Type_Range, Type_Client, Contacts_Min, Value_Min, Value_Max):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)

    BOT_list = ["SALDO INSOLUTO", "Resto de Rangos"] 

    for Type_Proccess in BOT_list:
        BOT_Process(Data_Frame, Type_Range, Type_Client, output_directory, \
                    Partitions, Type_Proccess, Contacts_Min, Value_Min, Value_Max)

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

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions, Type_Range, Type_Proccess):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"BOT_GM_{Type_Proccess}_"

    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    Partitions = int(Partitions)
    Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").csv(output_path)

    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                os.remove(os.path.join(root, file))
    
    for i, file in enumerate(os.listdir(output_path), start=1):
        if file.endswith(".csv"):
            old_file_path = os.path.join(output_path, file)
            new_file_path = os.path.join(output_path, f'BOT Part- {i}.csv')
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

def outstanding_balance(RDD):
    
    Price_Col = "deuda_total"     
    
    RDD = RDD.filter(col("rango") == "SALDO INSOLUTO")
    RDD = RDD.withColumn("cuenta", col("cuenta").cast("string"))
    
    RDD = RDD.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    
    for col_name, data_type in RDD.dtypes:
        if data_type == "double":
            RDD = RDD.withColumn(col_name, col(col_name).cast(StringType()))

    RDD = RDD.dropDuplicates(["Cruce_Cuentas"])

    RDD = RDD.withColumn("PBX", lit("6017560290 OPC 2"))
    RDD = RDD.withColumn("PROCESO", lit("SALDO INSOLUTO"))
    RDD = RDD.withColumn("NOMBRE CAMPANA", lit("SALDO INSOLUTO"))
    RDD = RDD.withColumn("APELLIDO COMPLETO", lit(""))
    RDD = RDD.withColumn("MONTO", col(f"{Price_Col}"))
    RDD = RDD.withColumn("VALOR ADEUDADO", col(f"{Price_Col}"))

    RDD = RDD.withColumnRenamed("identificacion", "IDENTIFICACION")
    RDD = RDD.withColumnRenamed("nombrecompleto", "NOMBRE COMPLETO")
    RDD = RDD.withColumnRenamed("Dato_Contacto", "CELULAR")
    RDD = RDD.withColumnRenamed("placa", "PLACA")

    RDD = RDD.select("NOMBRE CAMPANA", "IDENTIFICACION", "NOMBRE COMPLETO", "APELLIDO COMPLETO", "CELULAR", "MONTO",\
                     "PROCESO", "VALOR ADEUDADO", "PLACA", "PBX", "dias_de_mora", "mejorperfil_mes", "Honorarios GM",\
                     "Descuento Rango")
    
    RDD = RDD.sort(col("IDENTIFICACION"), col("CELULAR"))

    return RDD

def range_others(RDD):
    
    Price_Col = "deuda_total"     
    
    RDD = RDD.filter(col("rango") != "SALDO INSOLUTO")
    RDD = RDD.withColumn("cuenta", col("cuenta").cast("string"))
    
    RDD = RDD.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    
    for col_name, data_type in RDD.dtypes:
        if data_type == "double":
            RDD = RDD.withColumn(col_name, col(col_name).cast(StringType()))

    RDD = RDD.dropDuplicates(["Cruce_Cuentas"])

    RDD = RDD.withColumn("PBX", lit("6017560290 OPC 2"))
    RDD = RDD.withColumn("PROCESO", col("tipo_de_cliente"))
    RDD = RDD.withColumn("NOMBRE CAMPANA", col("rango"))
    RDD = RDD.withColumn("APELLIDO COMPLETO", lit(""))
    RDD = RDD.withColumn("MONTO", col(f"{Price_Col}"))
    RDD = RDD.withColumn("VALOR ADEUDADO", col(f"{Price_Col}"))

    RDD = RDD.withColumnRenamed("identificacion", "IDENTIFICACION")
    RDD = RDD.withColumnRenamed("nombrecompleto", "NOMBRE COMPLETO")
    RDD = RDD.withColumnRenamed("Dato_Contacto", "CELULAR")
    RDD = RDD.withColumnRenamed("placa", "PLACA")
    RDD = RDD.withColumnRenamed("fecha_castigo", "FECHA CASTIGO")

    RDD = RDD.select("NOMBRE CAMPANA", "IDENTIFICACION", "NOMBRE COMPLETO", "APELLIDO COMPLETO", "CELULAR", "MONTO",\
                     "PROCESO", "VALOR ADEUDADO", "PLACA", "FECHA CASTIGO", "PBX", "dias_de_mora", "mejorperfil_mes", "Honorarios GM",\
                     "Descuento Rango")
    
    RDD = RDD.sort(col("IDENTIFICACION"), col("CELULAR"))

    return RDD

### Proceso de mensajería
def BOT_Process (Data_, Type_Range, Type_Client, Directory_to_Save, Partitions, Type_Proccess, Contacts_Min, Value_Min, Value_Max):
    
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
    
    #Data_ = Data_.withColumn("deuda_total",(col("deuda_total") + ((col("deuda_total") * (col("Honorarios GM")/100)) / 100)))
    #Data_ = Data_.withColumn("deuda_total",((col("deuda_total") * col("Descuento Rango")) / 100))

    Data_ = Function_Filter(Data_, Contacts_Min, Value_Min, Value_Max)    
    
    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))

    windowSpec = Window.partitionBy("identificacion").orderBy("cuenta","Dato_Contacto")
    Data_ = Data_.withColumn("Filtro", row_number().over(windowSpec))    

    if Type_Proccess == "Resto de Rangos":
        Data_ = range_others(Data_)

    else:
        Data_ = outstanding_balance(Data_)

    Save_Data_Frame(Data_, Directory_to_Save, Partitions, Type_Range, Type_Proccess)
    
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
Type_Range = ["R3", "R4", "SALDO INSOLUTO"]
Type_Client = ["Comercial", "Integral", "Nulo"]
Contacts_Min = "Todos"

Value_Min = 0
Value_Max = 99999999999999

Function_Complete(path, output_directory, Partitions, Type_Range, Type_Client, Contacts_Min, Value_Min, Value_Max)