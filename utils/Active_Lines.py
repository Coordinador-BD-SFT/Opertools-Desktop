import os
from skills import List_City_Mins
import string
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, expr

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, partitions):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)

    Data_NO = Demographic_Proccess(Data_Frame, output_directory, partitions, "NO_valido")
    Data_AC = Demographic_Proccess(Data_Frame, output_directory, partitions, "valido")

    Data_Frame = Data_AC.union(Data_NO)

    Save_Data_Frame(Data_Frame, output_directory, partitions)

### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True,sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    return DF

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("1_", "identificacion")
    Data_Frame = Data_Frame.withColumnRenamed("2_", "cuenta")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, partitions):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Time_File_File = now.strftime("%Y%m%d")
    Type_File = f"Demograficos_de_Cargue_"
    
    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    partitions = int(partitions)

    Data_Frame = Data_Frame.withColumn("cruice", concat(col("cuenta"), col("dato")))
    Data_Frame = Data_Frame.dropDuplicates(["cruice"])

    Data_Frame = Data_Frame.select("identificacion", "cuenta", "ciudad", "depto", "dato", "tipodato")

    Data_Frame = Data_Frame.orderBy(col("dato"))

    Data_Frame.repartition(partitions).write.mode("overwrite").option("header", "true").option("delimiter",";").csv(output_path)

    for root, dirs, files in os.walk(output_path):
            for file in files:
                if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                    os.remove(os.path.join(root, file))
        
    for i, file in enumerate(os.listdir(output_path), start=1):
        if file.endswith(".csv"):
            old_file_path = os.path.join(output_path, file)
            new_file_path = os.path.join(output_path, f'Demograficos {Time_File_File} {i}.csv')
            os.rename(old_file_path, new_file_path)

    return Data_Frame

### Dinamización de columnas de contacto
def Phone_Data(Data_):

    #Data_ = Data_.filter(col("7_") == "N")  ###Filter BRAND ###################

    list_replace = ["VDK", "VD"]
    
    #Data_ = Data_.filter((col("28_").like("%VDK%")) | (col("28_").like("%VD%")))

    for letters in list_replace:
        Data_ = Data_.withColumn("28_", regexp_replace(col("28_"), \
            letters, "9999999999"))
          
          ##  Replace for exclusion of mins

    columns_to_stack_min = ["28_"] #MIN
    columns_to_stack_mobile = ["47_", "48_", "49_", "50_"] #Telefono X
    columns_to_stack_activelines = ["52_", "53_", "54_", "55_", "56_"] #ActiveLines

    all_columns_to_stack = columns_to_stack_mobile + columns_to_stack_activelines + columns_to_stack_min

    columns_to_drop_contact = all_columns_to_stack
    stacked_contact_data_frame = Data_.select("*", *all_columns_to_stack)

    stacked_contact_data_frame = stacked_contact_data_frame.select(
        "*",
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as dato")
    )

    Data_ = stacked_contact_data_frame.drop(*columns_to_drop_contact)

    return Data_

def Remove_Dots(dataframe, column):

    dataframe = dataframe.withColumn(column, regexp_replace(col(column), "[.-]", ""))
    
    return dataframe

### Proceso de filtrado de líneas
def Demographic_Proccess(Data_, Directory_to_Save, partitions, TypeProccess):

    Data_ = Data_.withColumn("ciudad", lit("BOGOTA"))
    Data_ = Data_.withColumn("depto", lit("BOGOTA"))
    Data_ = Data_.withColumn("tipodato", lit("telefono"))
    
    Data_ = Data_.select("1_", "2_", "22_", "ciudad", "depto", "dato", "tipodato")
    
    character_list = list(string.ascii_uppercase)
    Punctuation_List = ["\\*"]
    character_list = character_list + Punctuation_List
    
    Data_ = Data_.withColumn("1_", upper(col("1_")))

    for character in character_list:
        Data_ = Data_.withColumn("1_", regexp_replace(col("1_"), character, ""))
        Data_ = Data_.withColumn("2_", regexp_replace(col("2_"), character, ""))
        Data_ = Data_.withColumn("dato", regexp_replace(col("dato"), character, ""))
    
    Data_ = Function_Filter(Data_, TypeProccess)
    Data_ = Data_.withColumn("cruice", concat(col("2_"), col("dato")))
    Data_ = Data_.dropDuplicates(["cruice"])

    Data_ = Remove_Dots(Data_, "1_")
    Data_ = Remove_Dots(Data_, "2_")

    Data_ = Renamed_Column(Data_)

    Data_ = Data_.select("identificacion", "cuenta", "ciudad", "depto", "dato", "tipodato")

    return Data_

def Function_Filter(RDD, TypeProccess):

    if TypeProccess == "valido":

        Data_C = RDD.filter(col("dato") >= 3000000001)
        Data_C = Data_C.filter(col("dato") <= 3599999998)
        Data_F = RDD.filter(col("dato") >= 6010000000)
        Data_F = Data_F.filter(col("dato") <= 6089999998)
    
        RDD = Data_C.union(Data_F)
    
    else:

        RDD = List_City_Mins.lines_inactives_df(RDD)

    return RDD