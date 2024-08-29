import os
import utils.Active_Lines
from datetime import datetime
from PyQt6.QtCore import QDate
from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QMessageBox
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, expr, when, coalesce

class Charge_DB(QtWidgets.QMainWindow):

    def __init__(self, row_count, file_path, folder_path, process_data):
        super().__init__()
        
        self.spinBox_Partitions = None
        self.partitions = None

        self.file_path = file_path
        self.folder_path = folder_path
        self.process_data = process_data
        self.digit_partitions()
        self.exec_process()

    def digit_partitions(self):

        partitions_CAM = self.process_data.spinBox_Partitions.value()
        print(partitions_CAM)
        self.partitions = partitions_CAM

    def exec_process(self):
        
        self.digit_partitions()
        self.data_to_process = []
        self.process_data.pushButton_CAM.clicked.connect(self.generate_DB)
        self.process_data.pushButton_Partitions_BD.clicked.connect(self.Partitions_Data_Base)
        self.process_data.pushButton_MINS.clicked.connect(self.mins_from_bd)

    def generate_DB(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        self.BD_Control_Next()
        self.DB_Create()

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de creación ejecutado exitosamente.")
        Mbox_In_Process.exec()

    def Partitions_Data_Base(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        self.partition_DATA()

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de partición ejecutado exitosamente.")
        Mbox_In_Process.exec()

    def mins_from_bd(self):

        path =  self.file_path
        output_directory = self.folder_path
        partitions = self.partitions

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        utils.Active_Lines.Function_Complete(path, output_directory, partitions)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de valdiación de líneas ejecutado exitosamente.")
        Mbox_In_Process.exec()
    
    def BD_Control_Next(self):

        spark = SparkSession \
            .builder.appName("BD_CN") \
            .getOrCreate()
        spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

        sqlContext = SQLContext(spark)
        
        list_data = [self.file_path, self.folder_path, self.partitions]

        file = list_data[0]
        root = list_data[1]
        partitions = int(list_data[2])

        list_origins = ["ASCARD", "RR", "BSCS", "SGA"]

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")

        Data_Root = spark.read.csv(file, header= True, sep=";")
        Data_Root = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

        columns_to_list = [f"{i}_" for i in range(1, 53)]
        Data_Root = Data_Root.select(columns_to_list)
        Data_Root = Data_Root.filter(col("3_").isin(list_origins))

        ##############
        #Data_Root = Data_Root.filter(col("7_") != "Y")      #Filter Brand CASTIGO
        ##############

        Data_Root = Data_Root.withColumn("Telefono 1", lit(""))
        Data_Root = Data_Root.withColumn("Telefono 2", lit(""))
        Data_Root = Data_Root.withColumn("Telefono 3", lit(""))
        Data_Root = Data_Root.withColumn("Telefono 4", lit(""))
        Data_Root = Data_Root.withColumn("Valor Scoring", lit(""))
        Data_Root = Data_Root.withColumn("[AccountAccountCode2?]", col("2_"))

        columns_to_list = ["1_", "2_", "3_", "4_", "5_", "6_", "7_", "8_", "9_", "10_", "11_", "12_", \
                           "13_", "14_", "15_", "16_", "17_", "18_", "51_", "Telefono 1", "Telefono 2", "Telefono 3", \
                           "Telefono 4", "Valor Scoring", "19_", "20_", "21_", "22_", "23_", "24_", "25_", \
                           "26_", "27_", "28_", "29_", "30_", "31_", "32_", "33_", "34_", "35_", "36_", "37_", \
                           "38_", "39_", "40_", "41_", "42_", "43_", "44_", "[AccountAccountCode2?]"]
        
        Data_Root = Data_Root.select(columns_to_list)
        Data_Root = Data_Root.orderBy(col("3_"))

        Data_Root = Data_Root.withColumnRenamed("1_", "Numero de Cliente")
        Data_Root = Data_Root.withColumnRenamed("2_", "[AccountAccountCode?]")
        Data_Root = Data_Root.withColumnRenamed("3_", "CRM Origen")
        Data_Root = Data_Root.withColumnRenamed("4_", "Edad de Deuda")
        Data_Root = Data_Root.withColumnRenamed("5_", "[PotencialMark?]")
        Data_Root = Data_Root.withColumnRenamed("6_", "[PrePotencialMark?]")
        Data_Root = Data_Root.withColumnRenamed("7_", "[WriteOffMark?]")
        Data_Root = Data_Root.withColumnRenamed("8_", "Monto inicial")
        Data_Root = Data_Root.withColumnRenamed("9_", "[ModInitCta?]")
        Data_Root = Data_Root.withColumnRenamed("10_", "[DeudaRealCuenta?]")
        Data_Root = Data_Root.withColumnRenamed("11_", "[BillCycleName?]")
        Data_Root = Data_Root.withColumnRenamed("12_", "Nombre Campana")
        Data_Root = Data_Root.withColumnRenamed("13_", "[DebtAgeInicial?]")
        Data_Root = Data_Root.withColumnRenamed("14_", "Nombre Casa de Cobro")
        Data_Root = Data_Root.withColumnRenamed("15_", "Fecha de Asignacion")
        Data_Root = Data_Root.withColumnRenamed("16_", "Deuda Gestionable")
        Data_Root = Data_Root.withColumnRenamed("17_", "Direccion Completa")
        Data_Root = Data_Root.withColumnRenamed("18_", "Fecha Final ")
        Data_Root = Data_Root.withColumnRenamed("51_", "Email")
        Data_Root = Data_Root.withColumnRenamed("19_", "Segmento")
        Data_Root = Data_Root.withColumnRenamed("20_", "[Documento?]")
        Data_Root = Data_Root.withColumnRenamed("21_", "[AccStsName?]")
        Data_Root = Data_Root.withColumnRenamed("22_", "Ciudad")
        Data_Root = Data_Root.withColumnRenamed("23_", "[InboxName?]")
        Data_Root = Data_Root.withColumnRenamed("24_", "Nombre del Cliente")
        Data_Root = Data_Root.withColumnRenamed("25_", "Id de Ejecucion")
        Data_Root = Data_Root.withColumnRenamed("26_", "Fecha de Vencimiento")
        Data_Root = Data_Root.withColumnRenamed("27_", "Numero Referencia de Pago")
        Data_Root = Data_Root.withColumnRenamed("28_", "MIN")
        Data_Root = Data_Root.withColumnRenamed("29_", "Plan")
        Data_Root = Data_Root.withColumnRenamed("30_", "Cuotas Aceleradas")
        Data_Root = Data_Root.withColumnRenamed("31_", "Fecha de Aceleracion")
        Data_Root = Data_Root.withColumnRenamed("32_", "Valor Acelerado")
        Data_Root = Data_Root.withColumnRenamed("33_", "Intereses Contingentes")
        Data_Root = Data_Root.withColumnRenamed("34_", "Intereses Corrientes Facturados")
        Data_Root = Data_Root.withColumnRenamed("35_", "Intereses por mora facturados")
        Data_Root = Data_Root.withColumnRenamed("36_", "Cuotas Facturadas")
        Data_Root = Data_Root.withColumnRenamed("37_", "Iva Intereses Contigentes Facturado")
        Data_Root = Data_Root.withColumnRenamed("38_", "Iva Intereses Corrientes Facturados")
        Data_Root = Data_Root.withColumnRenamed("39_", "Iva Intereses por Mora Facturado")
        Data_Root = Data_Root.withColumnRenamed("40_", "Precio Subscripcion")
        Data_Root = Data_Root.withColumnRenamed("41_", "Codigo de proceso")
        Data_Root = Data_Root.withColumnRenamed("42_", "[CustomerTypeId?]")
        Data_Root = Data_Root.withColumnRenamed("43_", "[RefinanciedMark?]")
        Data_Root = Data_Root.withColumnRenamed("44_", "[Discount?]")

        Data_Error = Data_Root

        Data_Root = Data_Root.filter(col("[CustomerTypeId?]") >= 0)
        Data_Root = Data_Root.filter(col("[CustomerTypeId?]") <= 100)
        name = "Cargue"
        origin = "Multiorigen"
        self.Save_File(Data_Root, root, partitions, name, origin, Time_File)

        Data_Brands = Data_Root.filter(col("[WriteOffMark?]") != "Y")
        name = "Multimarca_Cargue"
        origin = "Multiorigen"
        self.Save_File(Data_Brands, root, partitions, name, origin, Time_File)

        Data_Error = Data_Error.filter((col("[CustomerTypeId?]").isNull()) & (col("[CustomerTypeId?]").cast("double").isNull()))
        name = "Errores"
        origin = "Multiorigen"
        self.Save_File(Data_Error, root, partitions, name, origin, Time_File)

        return Data_Root

    def DB_Create(self):
        
        list_data = [self.file_path, self.folder_path, self.partitions]

        file = list_data[0]
        root = list_data[1]
        partitions = int(list_data[2])

        list_brands = ["different", "castigo"]
        list_origins = ["ASCARD", "RR", "BSCS", "SGA"]

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")

        for brand in list_brands:

            if brand == "castigo":

                origin_list = list_origins
                origin = "Multiorigen"
                RDD_Data = self.Function_Complete(file, brand, origin_list)
                self.Save_File(RDD_Data, root, partitions, brand, origin, Time_File)

                for position in range(4):

                    origin_list = [list_origins[position]]
                    origin = f"{list_origins[position]}"
                    RDD_Data = self.Function_Complete(file, brand, origin_list)
                    self.Save_File(RDD_Data, root, partitions, brand, origin, Time_File)

                    if position % 2 != 0:

                        origin_list = [list_origins[position-1], list_origins[position]]
                        origin = f"{list_origins[position-1]} - {list_origins[position]}"
                        RDD_Data = self.Function_Complete(file, brand, origin_list)
                        self.Save_File(RDD_Data, root, partitions, brand, origin, Time_File)
                    
                    else:
                        pass

            else:
                origin_list = list_origins
                origin = "Multiorigen"
                RDD_Data = self.Function_Complete(file, brand, origin_list)
                self.Save_File(RDD_Data, root, partitions, brand, origin, Time_File)


    def Function_Complete(self, path, brand_filter, origin_filter):

        spark = SparkSession \
            .builder.appName("Trial") \
            .getOrCreate()
        spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

        sqlContext = SQLContext(spark)

        Data_Root = spark.read.csv(path, header= True, sep=";")
        Data_Root = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

        #ActiveLines
        Data_Root = Data_Root.withColumn("52_", concat(col("53_"), lit(","), col("54_"), lit(", "), col("55_"), lit(","), col("56_")))

        columns_to_list = [f"{i}_" for i in range(1, 53)]
        Data_Root = Data_Root.select(columns_to_list)
        
        potencial = (col("5_") == "Y") & (col("3_") == "BSCS")
        churn = (col("5_") == "Y") & ((col("3_") == "RR") | (col("3_") == "SGA"))
        provision = (col("5_") == "Y") & (col("3_") == "ASCARD")
        prepotencial = (col("6_") == "Y") & (col("3_") == "BSCS")
        prechurn = (col("6_") == "Y") & ((col("3_") == "RR") | (col("3_") == "SGA"))
        preprovision = (col("6_") == "Y") & (col("3_") == "ASCARD")
        castigo = col("7_") == "Y"
        potencial_a_castigar = (col("5_") == "N") & (col("6_") == "N") & (col("7_") == "N") & (col("43_") == "Y")
        marcas = col("13_")

        Data_Root = Data_Root.filter(col("3_").isin(origin_filter))

        Data_Root = Data_Root.withColumn("53_", when(potencial, "Potencial")\
                                            .when(churn, "Churn")\
                                            .when(provision, "Provision")\
                                            .when(prepotencial, "Prepotencial")\
                                            .when(prechurn, "Prechurn")\
                                            .when(preprovision, "Preprovision")\
                                            .when(castigo, "Castigo")\
                                            .when(potencial_a_castigar, "Potencial a Castigar")\
                                            .otherwise(marcas))
        
        moras_numericas = (col("53_") == "120") | (col("53_") == "150") | (col("53_") == "180")
        prepotencial_especial = (col("53_") == "Prepotencial") & (col("3_") == "BSCS") & ((col("12_") == "PrePotencial Convergente Masivo_2") | (col("12_") == "PrePotencial Convergente Pyme_2"))

        Data_Root = Data_Root.withColumn("53_", when(moras_numericas, "120 - 180")\
                                            .when(prepotencial_especial, "Prepotencial Especial")\
                                            .otherwise(col("53_")))

        if brand_filter == "castigo":
            Data_Root = Data_Root.filter(col("53_") == "Castigo")
        
        else: 
            Data_Root = Data_Root.filter(col("53_") != "Castigo")

        Data_Root = Data_Root.withColumn("54_", regexp_replace(col("2_"), "[.-]", ""))

        Data_Root = Data_Root.withColumn("55_", col("9_").cast("double"))

        Data_Root = Data_Root.withColumn("55_", regexp_replace("55_", "\\.", ","))
        
        Segment = ((col("42_") == "81") | (col("42_") == "84") | (col("42_") == "87"))
        Data_Root = Data_Root.withColumn("56_",
                          when(Segment, "Personas")
                          .otherwise("Negocios"))

        Data_Root = Data_Root.withColumn("57_", \
            when((col("9_") <= 20000), lit("1 Menos a 20 mil")) \
                .when((col("9_") <= 50000), lit("2 Entre 20 a 50 mil")) \
                .when((col("9_") <= 100000), lit("3 Entre 50 a 100 mil")) \
                .when((col("9_") <= 150000), lit("4 Entre 100 a 150 mil")) \
                .when((col("9_") <= 200000), lit("5 Entre 150 mil a 200 mil")) \
                .when((col("9_") <= 300000), lit("6 Entre 200 mil a 300 mil")) \
                .when((col("9_") <= 500000), lit("7 Entre 300 mil a 500 mil")) \
                .when((col("9_") <= 1000000), lit("8 Entre 500 mil a 1 Millon")) \
                .when((col("9_") <= 2000000), lit("9 Entre 1 a 2 millones")) \
                .otherwise(lit("9.1 Mayor a 2 millones")))
        
        Data_Root = Data_Root.orderBy(col("3_"))
        
        return Data_Root

    def Save_File(self, Data_Frame, Directory_to_Save, Partitions, Brand_Filter, Origin_Filter, Time_File):
        
        if Brand_Filter == "castigo":
            Type_File = f"BD_Castigo_{Time_File}/BD_Castigo_({Origin_Filter})_"
            extension = "0csv"
            Name_File = "Castigo"
        
        elif Brand_Filter == "Cargue" or Brand_Filter == "Errores" or Brand_Filter == "Multimarca_Cargue":
            Type_File = f"Base_de_CARGUE_{Time_File}"
            extension = "csv"

            if Brand_Filter == "Errores":
                Type_File = f"Base_de_CARGUE_{Time_File}/Errores"
                Name_File = "de Errores (NO RELACIONADA EN CARGUE)"
                extension = "0csv"

            elif Brand_Filter == "Multimarca_Cargue":
                Type_File = f"Base_de_CARGUE_{Time_File}/Cargue sin Castigo"
                Name_File = "UNIF sin Castigo"

            else:
                Name_File = "Cargue UNIF"

        else: 
            Type_File = f"BD_Multimarca_{Time_File}"
            extension = "0csv"
            Name_File = "Multimarca"
        
        output_path = f'{Directory_to_Save}{Type_File}'
        Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").option("delimiter",";").csv(output_path)
        
        for root, dirs, files in os.walk(output_path):
            for file in files:
                if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                    os.remove(os.path.join(root, file))
        
        for i, file in enumerate(os.listdir(output_path), start=1):
            if file.endswith(".csv"):
                old_file_path = os.path.join(output_path, file)
                new_file_path = os.path.join(output_path, f'BASE {Name_File} Part- {i}.{extension}')
                os.rename(old_file_path, new_file_path)

    def partition_DATA(self):

        list_data = [self.file_path, self.folder_path, self.partitions]

        file = list_data[0]
        root = list_data[1]
        partitions = int(list_data[2])

        try:
            with open(file, 'r', encoding='utf-8') as origin_file:
                rows = origin_file.readlines()

                rows_por_particion = len(rows) // partitions

                for i in range(partitions):
                    begining = i * rows_por_particion
                    end = (i + 1) * rows_por_particion if i < partitions - 1 else len(rows)

                    if i > 0:
                        extension_file = "0csv"
                    else:
                        extension_file = "csv"

                    nombre_particion = os.path.join(root, f"Particion {i+1}.{extension_file}")

                    with open(nombre_particion, 'w', encoding='utf-8') as file_output:
                        file_output.writelines(rows[begining:end])

                if end < len(rows):
                    nombre_particion = os.path.join(root, f"File_Part_{partitions+1}.csv")
                    with open(nombre_particion, 'w', encoding='utf-8') as file_output:
                        file_output.writelines(rows[end:])

        except:
            pass

    
    
