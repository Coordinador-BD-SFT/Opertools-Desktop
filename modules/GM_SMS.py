import os
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, length, split
from pyspark.sql.functions import trim, format_number, expr, when, coalesce, datediff, current_date

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, Partitions, Type_Range, Type_Client, Contacts_Min, Value_Min, Value_Max):
    
    Type_Proccess = "NORMAL"
    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = change_name_column(Data_Frame, "nombrecompleto")
    Data_Frame = SMS_Proccess(Data_Frame, Type_Range, output_directory, Type_Proccess, Partitions, Type_Client, \
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

    Data_Frame = Data_Frame.withColumnRenamed("cuenta", "Cuenta_Next")
    Data_Frame = Data_Frame.withColumnRenamed("identificacion", "Identificacion")
    Data_Frame = Data_Frame.withColumnRenamed("cuenta2", "Cuenta")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_asignacion", "Fecha_Asignacion")
    Data_Frame = Data_Frame.withColumnRenamed("marca", "Edad_Mora")
    Data_Frame = Data_Frame.withColumnRenamed("tipo_de_cliente", "CRM")
    Data_Frame = Data_Frame.withColumnRenamed("deuda_total", "Saldo_Asignado")
    Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "Nombre_Completo")
    Data_Frame = Data_Frame.withColumnRenamed("min", "Min")
    Data_Frame = Data_Frame.withColumnRenamed("referencia", "Referencia")
    Data_Frame = Data_Frame.withColumnRenamed("Fecha_Hoy", "Fecha_Envio")
    Data_Frame = Data_Frame.withColumnRenamed("customer_type_id", "Segmento")
    Data_Frame = Data_Frame.withColumnRenamed("descuento", "DCTO")
    Data_Frame = Data_Frame.withColumnRenamed("tipo_pago", "TIPO_PAGO")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_vencimiento", "FLP")
    Data_Frame = Data_Frame.withColumnRenamed("mejorperfil_mes", "MEJOR PERFIL")
    Data_Frame = Data_Frame.withColumnRenamed("dias_transcurridos", "DIAS DE MORA")

    Data_Frame = Data_Frame.select("Identificacion", "Cuenta_Next", "Cuenta", "Fecha_Asignacion", "Edad_Mora", \
                                   "CRM", "Saldo_Asignado", "Segmento",	"Form_Moneda", "Nombre_Completo", "Rango", \
                                    "Referencia", "Min", "Dato_Contacto", "Canal", "Hora_Envio", "Hora_Real", \
                                    "Fecha_Envio", "DCTO", "DEUDA_REAL", "FLP", "PRODUCTO", "fechapromesa", \
                                    "TIPO_PAGO", "MEJOR PERFIL", "DIAS DE MORA", "NOMBRE CORTO")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions, Type_Range):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"Mensajeria_GM_"

    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").csv(output_path)
    
    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                os.remove(os.path.join(root, file))
    
    for i, file in enumerate(os.listdir(output_path), start=1):
        if file.endswith(".csv"):
            old_file_path = os.path.join(output_path, file)
            new_file_path = os.path.join(output_path, f'SMS Part- {i}.csv')
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

### Proceso de mensajería
def SMS_Proccess (Data_Frame, Type_Range, output_directory, Type_Proccess, Partitions, Type_Client, \
                              Contacts_Min, Value_Min, Value_Max):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"SMS__"

    Data_ = Data_Frame
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
    Data_ = Data_.withColumn("Canal", lit(f"SMS_{Type_Proccess}"))

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

    Data_ = Data_.withColumn("Honorarios GM", col("Honorarios GM") / 100)
                             
    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))
    Data_ = Data_.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.withColumn("Form_Moneda", concat(lit("$ "), format_number(col(Price_Col), 0)))
    
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
    
    Data_ = Data_.withColumn("Hora_Envio", lit(now.strftime("%H")))
    Data_ = Data_.withColumn("Hora_Real", lit(now.strftime("%H:%M")))
    Data_ = Data_.withColumn("Fecha_Hoy", lit(now.strftime("%d/%m/%Y")))

    Data_ = Data_.dropDuplicates(["Cruce_Cuentas"])

    Data_ = Data_.withColumn(
        "etapa_procesal", 
        when(col("etapa_procesal").isNull(), lit("NULO"))
        .otherwise(col("etapa_procesal")))

    Data_ = Data_.select("identificacion", "cuenta", "fecha_castigo", "rango", "placa", \
                         "tipo_de_cliente", f"{Price_Col}", "estado", "Form_Moneda", "nombrecompleto", \
                        "Rango_Deuda", "Dato_Contacto", "Canal", "Hora_Envio", "Hora_Real", \
                        "Fecha_Hoy", "DEUDA_REAL", "dias_de_mora", "mejorperfil_mes", "Honorarios GM",\
                        "Descuento Rango")
    
    Data_ = Data_.withColumn("NOMBRE CORTO", col("nombrecompleto"))

    Data_ = Data_.withColumn("NOMBRE CORTO", split(col("NOMBRE CORTO"), " "))

    for position in range(4):
        Data_ = Data_.withColumn(f"Name_{position}", (Data_["NOMBRE CORTO"][position]))

    Data_ = Data_.withColumn("NOMBRE CORTO", when(length(col("Name_1")) > 3, col("Name_1"))
                             .when(length(col("Name_2")) > 3, col("Name_2"))
                             .when(length(col("Name_0")) > 3, col("Name_0"))
                             .when(length(col("Name_3")) > 3, col("Name_3"))
                             .otherwise(col("Name_1")))

    Data_ = Data_.select("identificacion", "cuenta", "fecha_castigo", "rango", "placa",\
                         "tipo_de_cliente", f"{Price_Col}", "estado", "Form_Moneda", "nombrecompleto", \
                        "Rango_Deuda", "Dato_Contacto", "Canal", "Hora_Envio", "Hora_Real", \
                        "Fecha_Hoy", "DEUDA_REAL", "dias_de_mora", "mejorperfil_mes", "Honorarios GM",\
                        "Descuento Rango", "NOMBRE CORTO")
    
    #Data_ = Renamed_Column(Data_)
    Data_ = sms_gm_tables(Data_)
    Save_Data_Frame(Data_, output_directory, Partitions, Type_Range)
    
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

    Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
    Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
    RDD = Data_C
    
    RDD = RDD.filter(col("deuda_total") >= Value_Min)
    RDD = RDD.filter(col("deuda_total") <= Value_Max)

    return RDD

def sms_gm_tables(RDD):

    RDD = RDD.withColumn("SMS_1", lit("Apreciado Cliente aproveche la brigada de recuperacion que se llevara a cabo el dia __DIA_NEGOCIACION__ para negociar su credito de vehiculo con Chevrolet Servicios Financieros ¡Excelentes condonaciones! Lo esperamos en la CL 170 #20A-13 Bogota Toberin de 9am a 1pm. Agende su cita aquí http://wa.me/573125927022"))
    RDD = RDD.withColumn("SMS_2", lit("CHEVROLET SERVICIOS FINANCIEROS LE OFRECE MAS DESCUENTOS y le facilita el pago de su credito de vehiculo con una increíble condonacion. Aproveche para realizar su acuerdo de pago y con un descuento HASTA DEL 50%. NO PIERDA ESTA OPORTUNIDAD COMUNIQUESE YA http://wa.me/573125927022"))
    RDD = RDD.withColumn("SMS_3", concat(lit("Su obligacion del vehiculo con placas "), col("placa"), lit(" se encuentra en proceso juridico, CHEVROLET SERVICIOS FINANCIEROS Y RECUPERA SAS lo invita a concretar la negociacion. Comuniquese a la línea 3208780696 http://wa.me/573125927022")))
    RDD = RDD.withColumn("SMS_4", lit("En piloto"))
    RDD = RDD.withColumn("SMS_5", lit("En piloto"))
    RDD = RDD.withColumn("SMS_6", lit("En piloto"))

    return RDD

File_Name = "reporte_clientes_GM"
path = f'C:/Users/c.operativo/Downloads/{File_Name}.csv'
output_directory = 'C:/Users/c.operativo/Downloads/'

Partitions = 1
Type_Range = ["R3","R4","SALDO INSOLUTO","R1","R2", "NULO"]
#Type_Range = ["R3", "R4", "SALDO INSOLUTO"]
Type_Client = ["Comercial", "Integral", "Nulo"]
Contacts_Min = "Celular"

Value_Min = 0
Value_Max = 99999999999999

Function_Complete(path, output_directory, Partitions, Type_Range, Type_Client, Contacts_Min, Value_Min, Value_Max)