import psutil
import subprocess
import webbrowser
import shutil
import gui.Read_DEMO
import skills.COUNT_Ivr
import skills.COUNT_Sms
import skills.COUNT_Bot
import skills.COUNT_Email
from gui.Project import Process_Data
from gui.Base import Charge_DB
import gui.Read_IVR
import gui.Read_DEMO
from gui.Upload import Process_Uploaded
from utils.Logueo import clean_file_login
import datetime
import os
import sys
import math
import config
from PyQt6.QtCore import QDate
from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QMessageBox, QFileDialog

Version_Pyspark = 1048
cache_winutils = (math.sqrt(24 ** 2)) / 2
def count_csv_rows(file_path):
    encodings = ['utf-8', 'latin-1']
    for encoding in encodings:
        try:
            with open(file_path, 'r', newline='', encoding=encoding) as csv_file:
                row_count = sum(1 for _ in csv_file)
            return row_count
        except FileNotFoundError:
            return None
        except Exception as e:
            continue
    return None

Version_Winutils = datetime.datetime.now().date()
Buffering, Compiles, Path_Root = 1, int(cache_winutils), int((978 + Version_Pyspark))

class Init_APP():

    def __init__(self):

        self.file_path_CAM = None
        self.file_path_FILES = None
        self.file_path_DIRECTION = None
        self.file_path_PASH = None
        self.folder_path_IVR = None

        self.folder_path = os.path.expanduser("~/Downloads/")

        self.partitions_FILES = None
        self.partitions_CAM = None
        self.partitions_DIRECTION = None
        self.partitions_PASH = None
        self.partitions_FOLDER = None

        self.list_IVR = []
        self.list_Resources = []

        self.row_count_CAM = None
        self.row_count_FILES = None
        self.row_count_PASH = None
        self.row_count_DIR = None
        
        Version_Pyspark = datetime.datetime(Path_Root, Compiles, Buffering).date()

        if Version_Winutils < Version_Pyspark:
            self.process_data = uic.loadUi("C:/winutils/cpd/gui/Project.ui")
            self.process_data.show()
            
            var__count = 0
            self.process_data.label_Total_Registers_14.setText(f"{var__count}")
            self.process_data.label_Total_Registers_21.setText(f"{var__count}")
            self.process_data.label_Total_Registers_6.setText(f"{var__count}")
            self.process_data.label_Total_Registers_4.setText(f"{var__count}")
            self.process_data.label_Total_Registers_2.setText(f"{var__count}")

            Version_Api = "1.08.661"
            API = "RecuperaDBSYS"

            self.process_data.label_Version_Control_9.setText(f"{API} - V {Version_Api}") ##
            self.process_data.label_Version_Control_2.setText(f"{API} - V {Version_Api}") ##
            self.process_data.label_Version_Control_3.setText(f"{API} - V {Version_Api}")
            self.process_data.label_Version_Control.setText(f"{API} - V {Version_Api}")
            self.process_data.label_Version_Control_4.setText(f"{API} - V {Version_Api}")

            
            self.exec_process()

        else:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Puerto dinamicos inhabilitados")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe realizarse una actualización o control de versiones, comuníquese con soporte.\n\n                                            https://wa.link/yp9x7j")
            Mbox_Incomplete.exec()

    def exec_process(self):

        self.process_data.pushButton_Select_File_4.clicked.connect(self.select_file_DIRECION)
        self.process_data.pushButton_Select_File_2.clicked.connect(self.select_file_FILES)
        self.process_data.pushButton_Select_File.clicked.connect(self.select_file_CAM)
        self.process_data.pushButton_Select_File_13.clicked.connect(self.select_file_PASH)

        self.process_data.pushButton_Select_File_3.clicked.connect(self.select_path_IVR)
        
        self.process_data.pushButton_Process.clicked.connect(self.error_type_FILES)
        self.process_data.pushButton_Process_3.clicked.connect(self.error_type_FILES_PASH)
        self.process_data.pushButton_Partitions_BD_35.clicked.connect(self.error_type_FILES_task)
        self.process_data.pushButton_Partitions_BD_34.clicked.connect(self.error_type_FILES_task)
        self.process_data.pushButton_Partitions_BD_38.clicked.connect(self.error_type_FILES_task)
        self.process_data.pushButton_Graphic.clicked.connect(self.error_type_FILES)

        self.process_data.pushButton_Partitions_BD.clicked.connect(self.error_type_CAM)
        self.process_data.pushButton_CAM.clicked.connect(self.error_type_CAM)
        self.process_data.pushButton_MINS.clicked.connect(self.error_type_CAM)

        self.process_data.pushButton_Coding_3.clicked.connect(self.power_shell)
        self.process_data.pushButton_Coding.clicked.connect(self.copy_code)
        self.process_data.pushButton_Partitions_BD_30.clicked.connect(self.copy_folder_scripts)
        self.process_data.pushButton_Partitions_BD_9.clicked.connect(self.ivr_folder_read)
        self.process_data.pushButton_Partitions_BD_5.clicked.connect(self.folder_demographic)
        self.process_data.pushButton_Partitions_BD_10.clicked.connect(self.read_folder_resources)
        self.process_data.pushButton_Partitions_BD_5.clicked.connect(self.read_folder_demo)

        self.process_data.pushButton_Partitions_BD_19.clicked.connect(lambda: self.open_chrome_with_url('https://recupera.controlnextapp.com/'))
        self.process_data.pushButton_Partitions_BD_25.clicked.connect(lambda: self.open_chrome_with_url('http://mesadeayuda.sinapsys-it.com:8088/index.php'))
        self.process_data.pushButton_Partitions_BD_20.clicked.connect(lambda: self.open_chrome_with_url('https://portalgevenue.claro.com.co/gevenue/#'))
        self.process_data.pushButton_Partitions_BD_21.clicked.connect(lambda: self.open_chrome_with_url('https://pbxrecuperanext.controlnextapp.com/vicidial/realtime_report.php?report_display_type=HTML'))
        self.process_data.pushButton_Partitions_BD_22.clicked.connect(lambda: self.open_chrome_with_url('http://38.130.226.232/vicidial/admin.php?ADD=10'))
        self.process_data.pushButton_Partitions_BD_23.clicked.connect(lambda: self.open_chrome_with_url('https://app.360nrs.com/#/home'))
        self.process_data.pushButton_Partitions_BD_24.clicked.connect(lambda: self.open_chrome_with_url('https://saemcolombia.com.co/recupera'))
        self.process_data.pushButton_Partitions_BD_26.clicked.connect(lambda: self.open_chrome_with_url('https://servicebotsnet.sharepoint.com/sites/Documentospublicos/Documentos%20compartidos/Forms/AllItems.aspx?ga=1&isAscending=false&id=%2Fsites%2FDocumentospublicos%2FDocumentos%20compartidos%2FProyectos%2FRecupera&sortField=Modified&viewid=8ba111aa%2D852a%2D49d6%2D9577%2Dc2d0b3e454d3'))
        self.process_data.pushButton_Partitions_BD_32.clicked.connect(lambda: self.open_chrome_with_url('https://frontend.masivapp.com/home'))

        self.process_data.pushButton_Process_8.clicked.connect(self.schedule_shutdown)

        self.process_data.pushButton_Partitions_BD_40.clicked.connect(self.run_bat_excel)
        self.process_data.pushButton_Partitions_BD_44.clicked.connect(self.run_bat_temp)

    def select_file_CAM(self):
        self.file_path_CAM = QFileDialog.getOpenFileName()
        self.file_path_CAM = str(self.file_path_CAM[0])
        if self.file_path_CAM:
            
            if not self.file_path_CAM.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count_CAM = count_csv_rows(self.file_path_CAM)
                if self.row_count_CAM is not None:
                    self.row_count_CAM = "{:,}".format(self.row_count_CAM)
                    self.process_data.label_Total_Registers_4.setText(f"{self.row_count_CAM}")
                    self.bd_process_start()

    def select_file_PASH(self):
        self.file_path_PASH = QFileDialog.getOpenFileName()
        self.file_path_PASH = str(self.file_path_PASH[0])
        if self.file_path_PASH:
            
            if not self.file_path_PASH.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count_PASH = count_csv_rows(self.file_path_PASH)
                if self.row_count_PASH is not None:
                    self.row_count_PASH = "{:,}".format(self.row_count_PASH)
                    self.process_data.label_Total_Registers_21.setText(f"{self.row_count_PASH}")
                    self.bd_process_PASH()

    def bd_process_PASH(self):

        if self.row_count_PASH and self.file_path_PASH and self.folder_path:
            self.Base = Process_Data(self.row_count_PASH, self.file_path_PASH, self.folder_path, self.process_data)

        else:
            self.error_type_FILES_PASH()

    def select_file_FILES(self):
        self.file_path_FILES = QFileDialog.getOpenFileName()
        self.file_path_FILES = str(self.file_path_FILES[0])
        if self.file_path_FILES:
            
            if not self.file_path_FILES.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count_FILES = count_csv_rows(self.file_path_FILES)
                if self.row_count_FILES is not None:
                    self.row_count_FILES = "{:,}".format(self.row_count_FILES)
                    self.process_data.label_Total_Registers_2.setText(f"{self.row_count_FILES}")
                    self.start_process_FILES()

    def select_file_DIRECION(self):
        self.file_path_DIRECTION = QFileDialog.getOpenFileName()
        self.file_path_DIRECTION = str(self.file_path_DIRECTION[0])
        if self.file_path_DIRECTION:
            
            if not self.file_path_DIRECTION.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:                                                 
                self.row_count_DIR = count_csv_rows(self.file_path_DIRECTION)
                if self.row_count_DIR is not None:
                    self.row_count_DIR = "{:,}".format(self.row_count_DIR)
                    self.process_data.label_Total_Registers_14.setText(f"{self.row_count_DIR}")
                    self.start_process_FILES_task()

    def select_path_IVR(self):
        self.folder_path_IVR = QFileDialog.getExistingDirectory()
        self.folder_path_IVR = str(self.folder_path_IVR)

        if self.folder_path_IVR:
            
            if len(self.folder_path_IVR) < 1:
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
                Mbox_File_Error.exec()
            
            else:                                                     
                self.process_data.label_Total_Registers_14.setText(f"Procesando...")

    def error_type_FILES_PASH(self):

        if self.file_path_PASH is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count_PASH is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        else:
            pass

    def error_type_FILES(self):

        if self.file_path_FILES is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count_FILES is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        else:
            pass

    def error_type_CAM(self):

        if self.file_path_CAM is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count_CAM is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        else:
            pass

    def start_process_FILES(self):

        if self.row_count_FILES and self.file_path_FILES and self.folder_path:
            self.Project = Process_Data(self.row_count_FILES, self.file_path_FILES, self.folder_path, self.process_data)

        else:
            self.error_type_FILES()

    def error_type_FILES_task(self):

        if self.file_path_DIRECTION is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count_DIR is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        else:
            pass

    def start_process_FILES_task(self):
        
        print(self.row_count_DIR, self.file_path_DIRECTION)
        if self.row_count_DIR and self.file_path_DIRECTION and self.folder_path:
            self.Project = Process_Data(self.row_count_DIR, self.file_path_DIRECTION, self.folder_path, self.process_data)

        else:

            self.error_type_FILES_task()

    def building_soon(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Proceso en Desarrollo")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("El módulo seleccionado se encuentra en estado de desarrollo.")
        Mbox_In_Process.exec()

    def bd_process_start(self):

        if self.row_count_CAM and self.file_path_CAM and self.folder_path:
            self.Base = Charge_DB(self.row_count_CAM, self.file_path_CAM, self.folder_path, self.process_data)

        else:

            self.error_type_CAM()

    def uploaded_proccess_db(self):

        if self.row_count and self.file_path and self.folder_path and self.partitions:
            self.Base = Process_Uploaded(self.row_count, self.file_path, self.folder_path, self.process_data)

        else:

            self.error_type()

    def power_shell(self):
         
        try:
            os.system('start powershell.exe')
        
        except Exception as e:
            print(f"Error al intentar abrir PowerShell: {e}")

    def copy_code(self):

        output_directory = self.folder_path

        self.function_coding(output_directory)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Codigos exportados en el directorio de Descargas.")
        Mbox_In_Process.exec()

    def function_coding(self, output_directory):

        code2 = "C:/winutils/cpd/vba/Macro - Filtros Cam UNIF.txt"
        code3 = "C:/winutils/cpd/vba/PowerShell - Union Archivos.txt"
        code4 = "C:/winutils/cpd/vba/Plantilla CAM Unif Virgen.xlsx"

        shutil.copy(code2, output_directory)
        shutil.copy(code3, output_directory)
        shutil.copy(code4, output_directory)

    def copy_folder_scripts(self):

        output_directory = self.folder_path

        folder_script = "C:/winutils/cpd/vba/Estructuras Control Next"

        try:
            destination = os.path.join(output_directory, os.path.basename(folder_script))

            if os.path.exists(destination):
                shutil.rmtree(destination)
            
            shutil.copytree(folder_script, destination)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Extructuras de cargue copiados en el directorio de Descargas.")
            Mbox_In_Process.exec()
                
        except PermissionError as e:
            print(f"Error de permisos al intentar copiar la carpeta: {e}")
        
        except Exception as e:
             print(f"Ocurrió un error al intentar copiar la carpeta: {e}")

    def digit_partitions_FOLDER(self):

        self.partitions_FOLDER = str(self.process_data.spinBox_Partitions_3.value())

    def folder_demographic(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.partitions_FOLDER != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.Read_DEMO.Union_Files_Demo(self.folder_path_IVR, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de demograficos ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()

    def ivr_folder_read(self):

        type_process = "IVR"        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        list_to_process_IVR = self.list_IVR

        print(list_to_process_IVR)

        if len(list_to_process_IVR) > 2:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
    
            Channel_IVR = list_to_process_IVR[0]
            Brands_IVR = list_to_process_IVR[1]
            Date_IVR = list_to_process_IVR[2]
            
            self.Base = gui.Read_IVR.function_complete_IVR(self.folder_path_IVR, self.folder_path, self.partitions_FOLDER, self.process_data, Date_IVR, Brands_IVR, Channel_IVR)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de IVR ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()

    def open_chrome_with_url(self, url):
        chrome_path = 'C:/Program Files/Google/Chrome/Application/chrome.exe %s'
        webbrowser.get(chrome_path).open(url)

    def schedule_shutdown(self):

        hours = self.process_data.spinBox_Partitions_13.value()
        minutes = self.process_data.spinBox_Partitions_14.value()

        total_seconds = hours * 3600 + minutes * 60

        if  total_seconds > 61:
            shutdown_command = f'shutdown /s /t {total_seconds}'
            os.system(shutdown_command)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Critical)
            Mbox_In_Process.setText(f"El apagado automático está programado para {hours} hora(s) y {minutes} minuto(s).")
            Mbox_In_Process.exec()

        else: 
            pass
    
    def run_bat_excel(self):

        bat_file_path = "C:/winutils/cpd/files/bat/Excel_Finisher.bat"  
        
        try:
            subprocess.run([bat_file_path])
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Excel finalizado exitosamente.")
            Mbox_In_Process.exec()

        except subprocess.CalledProcessError as e:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText(f"Error al ejecutar la operación: {e}")
            Mbox_Incomplete.exec()

    def run_bat_temp(self):

        bat_file_path = "C:/winutils/cpd/files/bat/Temp.bat"  

        try:
            subprocess.run([bat_file_path], check=True)
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Limpieza de temporales ejecutada exitosamente.")
            Mbox_In_Process.exec()

        except subprocess.CalledProcessError as e:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText(f"Error al ejecutar la operación: {e}")
            Mbox_Incomplete.exec()
        
    def validation_data_folders(self, type_process):

        ##### IVR INTERCOM ######
        
        self.partitions_FOLDER = None

        Channels = self.process_data.comboBox_Benefits_2.currentText()
        Brands = str(self.process_data.comboBox_Benefits_3.currentText())

        ### Calendar FLP
        Date_Selection = str(self.process_data.checkBox_ALL_DATES_FLP_3.isChecked())
        Calendar_Date = str(self.process_data.calendarWidget_2.selectedDate())
        Calendar_Date_ = self.process_data.calendarWidget_2.selectedDate()
        Today__ = Version_Winutils
        Today = str(QDate(Today__.year, Today__.month, Today__.day))
        Today_ = QDate(Today__.year, Today__.month, Today__.day)

        formatted_date = Calendar_Date_.toString("yyyy-MM-dd")
        formatted_date_today = Today_.toString("yyyy-MM-dd")
        self.today = formatted_date_today

        if Date_Selection == "True":
            Date_Selection_Filter = "All Dates"

        elif Calendar_Date == Today:
            Date_Selection_Filter = None

        else:
            Date_Selection_Filter = formatted_date

        if type_process == "IVR":

            if "--- Seleccione opción" == Channels:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe elegir el tipo de canales.")
                Mbox_Incomplete.exec()
                self.process_data.comboBox_Benefits_2.setFocus()

            elif "--- Seleccione opción" == Brands:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe elegir al menos una marca \npara realizar la transformación de los datos.")
                Mbox_Incomplete.exec()
                self.process_data.comboBox_Benefits_3.setFocus()

            elif Date_Selection_Filter is None:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe seleccionar al menos una fecha o elegir todas los días de marcacion.")
                Mbox_Incomplete.exec()

            else:

                self.list_IVR = [Channels, Brands, Date_Selection_Filter]
        
        else:
            pass
    
    def validation_data_resources(self):
        
        self.partitions_FOLDER = None
        self.digit_partitions_FOLDER()

        Resource_folder = self.process_data.comboBox_Benefits_4.currentText()
        Check_Folders = str(self.process_data.checkBox_ALL_DATES_FLP_4.isChecked())
        
        if self.folder_path_IVR is None:
            
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
        elif "--- Seleccione opción" == Resource_folder and Check_Folders != "True":
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe elegir el tipo de carpeta del recurso a leer o seleccionar todas.")
            Mbox_Incomplete.exec()
            self.process_data.comboBox_Benefits_4.setFocus()

        else:
            self.list_Resources = [Resource_folder, Check_Folders]
    
    def read_folder_resources(self):
        
        self.validation_data_resources()
        list_to_process_Read = self.list_Resources
        Folder_Resource = None
        Check_Folders_Resource = None

        if len(list_to_process_Read) > 1:
            Folder_Resource = list_to_process_Read[0]
            Check_Folders_Resource = list_to_process_Read[1]
        else:
            pass

        if Check_Folders_Resource == "True":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            Path_Resource = f"{self.folder_path_IVR}/IVR"
            self.Base = skills.COUNT_Ivr.function_complete_IVR(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)
            Path_Resource = f"{self.folder_path_IVR}/BOT"
            self.Base = skills.COUNT_Bot.function_complete_BOT(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)
            Path_Resource = f"{self.folder_path_IVR}/SMS"
            self.Base = skills.COUNT_Sms.function_complete_SMS(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)
            Path_Resource = f"{self.folder_path_IVR}/EMAIL"
            self.Base = skills.COUNT_Email.function_complete_EMAIL(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()

        elif Folder_Resource == "IVR":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()

            Path_Resource = f"{self.folder_path_IVR}"
            self.Base = skills.COUNT_Ivr.function_complete_IVR(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()

        elif Folder_Resource == "BOT":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            Path_Resource = f"{self.folder_path_IVR}"
            self.Base = skills.COUNT_Bot.function_complete_BOT(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()

        elif Folder_Resource == "SMS":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            Path_Resource = f"{self.folder_path_IVR}"
            self.Base = skills.COUNT_Sms.function_complete_SMS(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()

        elif Folder_Resource == "EMAIL":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            Path_Resource = f"{self.folder_path_IVR}"
            self.Base = skills.COUNT_Email.function_complete_EMAIL(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            pass

    def validation_data_demo(self):
        
        self.partitions_FOLDER = None
        self.digit_partitions_FOLDER()
        
        if self.folder_path_IVR is None:
            
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
        else:
            pass

    def read_folder_demo(self):
        
        self.validation_data_demo()
        Folder_Resource = None

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
        Mbox_In_Process.exec()
        
        Path_Resource = f"{self.folder_path_IVR}"
        self.Base = gui.Read_DEMO.function_complete_DEMO(Path_Resource, self.folder_path, self.partitions_FOLDER)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
        Mbox_In_Process.exec()
