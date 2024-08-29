import os
import modules.Filter_Base as Filter_Base
from skills import Letters_SMS
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

Pictionary_Brands = {"0": "CERO", "30": "TREINTA", "60": "SESENTA", "90": "NOVENTA", \
                         "potencial": "POTENCIAL", "prechurn": "PRECHURN", "castigo": "CASTIGO"}

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, Partitions, Wallet_Brand, Origins_Filter, Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter):
    
    Type_Proccess = "NORMAL"
    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = SMS_Proccess(Data_Frame, Wallet_Brand, output_directory, Type_Proccess, Partitions, Origins_Filter, \
                              Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter)
    
    return Data_Frame


### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True, sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
    DF = change_character_account(DF, "cuenta")
    DF = change_character_account(DF, "cuenta2")
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
    Data_Frame = Data_Frame.withColumnRenamed("origen", "CRM")
    Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "Saldo_Asignado")
    Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "Nombre_Completo")
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
                                    "Referencia", "Dato_Contacto", "Hora_Envio", "Hora_Real", \
                                    "Fecha_Envio", "marca2", "DCTO", "DEUDA_REAL", "FLP", "PRODUCTO", "fechapromesa", \
                                    "TIPO_PAGO", "MEJOR PERFIL", "DIAS DE MORA", "NOMBRE CORTO")

    return Data_Frame

##### Union data of corporative
def union_corp_data(Data_Frame):

    mins_corp = ["3212401799", "3212401799", "3142502303", "3142502303", "3232285419",
                "3118185075", "3155568194", "3009964685", "3224436427"]

    datacorp = [("10000000000", "123456789", "1.23456789", "2000-04-30", "Corp", "BSCS", "0", "CORPORATIVO", "$ 99.999", "COLABORADOR DE RECUPERA", "0", "1234567", "0", 3212401799,"SMS", "0", "0", "0", "50","0", "0", "Postpago", "0", "Pago Parcial", "Validado", "1000", "COLABORADOR"),
                ("10000000000", "123456789", "1.23456789", "2000-04-30", "Corp", "BSCS", "0", "CORPORATIVO", "$ 99.999", "COLABORADOR DE RECUPERA", "0", "1234567", "0", 3212401799,"SMS", "0", "0", "0", "50","0", "0", "Postpago", "0", "Pago Parcial", "Validado", "1000", "COLABORADOR"),
                ("10000000000", "123456789", "1.23456789", "2000-04-30", "Corp", "BSCS", "0", "CORPORATIVO", "$ 99.999", "COLABORADOR DE RECUPERA", "0", "1234567", "0", 3142502303,"SMS", "0", "0", "0", "50","0", "0", "Postpago", "0", "Pago Parcial", "Validado", "1000", "COLABORADOR"),
                ("10000000000", "123456789", "1.23456789", "2000-04-30", "Corp", "BSCS", "0", "CORPORATIVO", "$ 99.999", "COLABORADOR DE RECUPERA", "0", "1234567", "0", 3142502303,"SMS", "0", "0", "0", "50","0", "0", "Postpago", "0", "Pago Parcial", "Validado", "1000", "COLABORADOR"),
                ("10000000000", "123456789", "1.23456789", "2000-04-30", "Corp", "BSCS", "0", "CORPORATIVO", "$ 99.999", "COLABORADOR DE RECUPERA", "0", "1234567", "0", 3232285419,"SMS", "0", "0", "0", "50","0", "0", "Postpago", "0", "Pago Parcial", "Validado", "1000", "COLABORADOR"),
                ("10000000000", "123456789", "1.23456789", "2000-04-30", "Corp", "BSCS", "0", "CORPORATIVO", "$ 99.999", "COLABORADOR DE RECUPERA", "0", "1234567", "0", 3118185075,"SMS", "0", "0", "0", "50","0", "0", "Postpago", "0", "Pago Parcial", "Validado", "1000", "COLABORADOR"),
                ("10000000000", "123456789", "1.23456789", "2000-04-30", "Corp", "BSCS", "0", "CORPORATIVO", "$ 99.999", "COLABORADOR DE RECUPERA", "0", "1234567", "0", 3155568194,"SMS", "0", "0", "0", "50","0", "0", "Postpago", "0", "Pago Parcial", "Validado", "1000", "COLABORADOR"),
                ("10000000000", "123456789", "1.23456789", "2000-04-30", "Corp", "BSCS", "0", "CORPORATIVO", "$ 99.999", "COLABORADOR DE RECUPERA", "0", "1234567", "0", 3009964685,"SMS", "0", "0", "0", "50","0", "0", "Postpago", "0", "Pago Parcial", "Validado", "1000", "COLABORADOR"),
                ("10000000000", "123456789", "1.23456789", "2000-04-30", "Corp", "BSCS", "0", "CORPORATIVO", "$ 99.999", "COLABORADOR DE RECUPERA", "0", "1234567", "0", 3224436427,"SMS", "0", "0", "0", "50","0", "0", "Postpago", "0", "Pago Parcial", "Validado", "1000", "COLABORADOR")]
    
    columnscorp = ["Identificacion", "Cuenta_Next", "Cuenta", "Fecha_Asignlacion", "Edad_Mora", \
                    "CRM", "Saldo_Asignado", "Segmento", "Form_Moneda", "Nombre_Completo", "Rango", \
                    "Referencia", "Min", "Dato_Contacto", "Canal", "Hora_Envio", "Hora_Real", \
                    "Fecha_Envio", "DCTO", "DEUDA_REAL", "FLP", "PRODUCTO", "fechapromesa", \
                    "TIPO_PAGO", "MEJOR PERFIL", "DIAS DE MORA", "NOMBRE CORTO"]
    
    #Data_Corp = spark.createDataFrame(datacorp, columnscorp)
    #Data_Frame.printSchema()
    #Data_Corp.printSchema()
    #Data_Frame = Data_Frame.union(Data_Corp)

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions, Wallet_Brand, widget_filter):

    #Data_Frame = union_corp_data(Data_Frame)

    if widget_filter != "Tables":
        
        list_key = ["SMS", "TEXTO"]

        for i in list_key:

            if i == "TEXTO":
                Data_Frame = Letters_SMS.generate_sms_message_column(Data_Frame)
            else:
                Data_Frame = Data_Frame

            now = datetime.now()
            Time_File = now.strftime("%Y%m%d_%H%M")
            File_Date = now.strftime("%Y%m%d")
            Type_File = f"{i}_MULTIMARCA_"

            output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
            Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").csv(output_path)
            
            for root, dirs, files in os.walk(output_path):
                for file in files:
                    if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                        os.remove(os.path.join(root, file))
            
            for i, file in enumerate(os.listdir(output_path), start=1):
                if file.endswith(".csv"):
                    old_file_path = os.path.join(output_path, file)
                    new_file_path = os.path.join(output_path, f'SMS {File_Date} Part- {i}.csv')
                    os.rename(old_file_path, new_file_path)

    else:
        Data_Frame = Data_Frame
        
    return Data_Frame

### Dinamización de columnas de celulares
def Phone_Data(Data_):

    columns_to_stack_celular = [f"celular{i}" for i in range(1, 11)]
    columns_to_stack_fijo = [f"fijo{i}" for i in range(1, 4)]
    columns_to_stack_min = ["min"]
    all_columns_to_stack = columns_to_stack_celular + columns_to_stack_fijo + columns_to_stack_min
    columns_to_drop_contact = all_columns_to_stack
    Stacked_Data_Frame = Data_.select("*", *all_columns_to_stack)
    
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as Dato_Contacto")
        )
    
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop_contact)
    Stacked_Data_Frame = Data_.select("*")

    return Stacked_Data_Frame

### Proceso de mensajería
def SMS_Proccess (Data_Frame, Wallet_Brand, output_directory, Type_Proccess, Partitions, Origins_Filter, \
                              Dates, Benefits, Contacts_Min, Value_Min, Value_Max,  widget_filter):

    Data_Frame = Filter_Base.Function_Complete(Data_Frame)

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"SMS__"
    
    filter_cash = ["", "Pago Parcial", "Sin Pago"]
    Data_ = Data_Frame.filter((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""))


    Data_ = Data_.filter(col("marca").isin(Wallet_Brand))
    Data_ = Data_.filter(col("origen").isin(Origins_Filter))

    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))

    Price_Col = "Mod_init_cta"     

    Data_ = Data_.withColumn(f"DEUDA_REAL", col(f"{Price_Col}").cast("double").cast("int"))
    
    Data_ = Function_Filter(Data_, Dates, Benefits, Contacts_Min, Value_Min, Value_Max)

    Data_ = Data_.withColumn(
        "Mod_init_cta", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))

    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))
    Data_ = Data_.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.withColumn("Form_Moneda", concat(lit("$ "), format_number(col(Price_Col), 0)))
    
    Data_ = Data_.withColumn("Rango", \
            when((Data_.Mod_init_cta <= 20000), lit("1 Menos a 20 mil")) \
                .when((Data_.Mod_init_cta <= 50000), lit("2 Entre 20 a 50 mil")) \
                .when((Data_.Mod_init_cta <= 100000), lit("3 Entre 50 a 100 mil")) \
                .when((Data_.Mod_init_cta <= 150000), lit("4 Entre 100 a 150 mil")) \
                .when((Data_.Mod_init_cta <= 200000), lit("5 Entre 150 mil a 200 mil")) \
                .when((Data_.Mod_init_cta <= 300000), lit("6 Entre 200 mil a 300 mil")) \
                .when((Data_.Mod_init_cta <= 500000), lit("7 Entre 300 mil a 500 mil")) \
                .when((Data_.Mod_init_cta <= 1000000), lit("8 Entre 500 mil a 1 Millón")) \
                .when((Data_.Mod_init_cta <= 2000000), lit("9 Entre 1 a 2 millones")) \
                .otherwise(lit("9.1 Mayor a 2 millones")))
    
    Data_ = Data_.withColumn("Hora_Envio", lit(now.strftime("%H")))
    Data_ = Data_.withColumn("Hora_Real", lit(now.strftime("%H:%M")))
    Data_ = Data_.withColumn("Fecha_Hoy", lit(now.strftime("%d/%m/%Y")))

    Data_ = Data_.dropDuplicates(["Cruce_Cuentas"])

    Data_ = Data_.withColumn(
        "PRODUCTO",
        when(col("origen") == "BSCS", "Postpago")
        .when(col("origen") == "ASCARD", "Equipo")
        .when(col("origen") == "RR", "Hogar")
        .when(col("origen") == "SGA", "Negocios")
        .otherwise(col("origen"))
    )

    Data_ = Data_.select("identificacion", "cuenta", "cuenta2", "fecha_asignacion", "marca", \
                         "origen", f"{Price_Col}", "customer_type_id", "Form_Moneda", "nombrecompleto", \
                        "Rango", "referencia", "Dato_Contacto", "Hora_Envio", "Hora_Real", \
                        "Fecha_Hoy", "marca2", "descuento", "DEUDA_REAL", "fecha_vencimiento", "PRODUCTO", \
                        "fechapromesa", "tipo_pago", "mejorperfil_mes")
    
    Data_ = Data_.withColumn("now", current_date())
    Data_ = Data_.withColumn("dias_transcurridos", datediff(col("now"), col("fecha_vencimiento")))
    
    Data_ = Data_.withColumn("NOMBRE CORTO", col("nombrecompleto"))

    Data_ = Data_.withColumn("NOMBRE CORTO", split(col("NOMBRE CORTO"), " "))

    for position in range(4):
        Data_ = Data_.withColumn(f"Name_{position}", (Data_["NOMBRE CORTO"][position]))

    Data_ = Data_.withColumn("NOMBRE CORTO", when(length(col("Name_1")) > 3, col("Name_1"))
                             .when(length(col("Name_2")) > 3, col("Name_2"))
                             .when(length(col("Name_0")) > 3, col("Name_0"))
                             .when(length(col("Name_3")) > 3, col("Name_3"))
                             .otherwise(col("Name_1")))

    Data_ = Renamed_Column(Data_)
    Save_Data_Frame(Data_, output_directory, Partitions, Wallet_Brand, widget_filter)
    
    return Data_

def Function_Filter(RDD, Dates, Benefits, Contacts_Min, Value_Min, Value_Max):

    if Dates == "All Dates":
        pass
    else: 
        RDD = RDD.filter(col("fecha_vencimiento") == Dates)

    RDD = RDD.withColumn("Referencia",  when(col("origen") == "RR", col("cuenta")).otherwise(col("Referencia")))           
    RDD = RDD.filter(col("Referencia") != "")

    Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
    Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
    RDD = Data_C
    
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