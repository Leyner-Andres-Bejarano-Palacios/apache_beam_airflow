# -*- coding: utf-8 -*-


import os
import sys
from pathlib import Path
path_root_directory = str(os.path.dirname(os.path.abspath(__file__)))[:-5]
sys.path.insert(1, path_root_directory)
import uuid
import time
import types
import threading
import numpy as np
import pandas as pd
import apache_beam as beam
from apache_beam import pvalue
from datetime import datetime,date
from datetime import timedelta
from google.cloud import bigquery as bq
from helperfunctions import fn_divide_clean_dirty
from apache_beam.runners.runner import PipelineState
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions

class Validador():
    def __init__(self, config):
        self._vName = self.__class__.__name__
        self._vConfig = config

    @staticmethod
    def fn_check_completitud(element,key):
        if (element[key] is None  or str(element[key]).lower() == "none" or (element[key]).lower() == "null" or str(element[key]).replace(" ","") == ""):
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
        return element
      
    @staticmethod      
    def fn_check_date_format(element,key,dateFormat):
        if (element[key] is not None  and element[key] != "None" and element[key] != "null" and str(element[key]).replace(" ","") != ""): 
            try:
                datetime.strptime(element[key].replace(" ",""), dateFormat)
            except:
                element["validacionDetected"] = element["validacionDetected"] + str(key) +" tiene un formato de fecha invalida," 
            finally:
                return element
        else:
            return element

    @staticmethod
    def fn_check_bigger_than(element,key,valueToCompare):
        if (element[key] is not None  and\
            element[key] != "None" and\
            element[key] != "null") and\
            str(element[key]).replace(" ","") != ""  and\
            element[key].strip().replace(",","").replace(".","").isnumeric():
            correct = False
            if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None"):
                if float(element[key]) < float(valueToCompare):
                    element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" es menor que "+str(valueToCompare)+","
        return element

    @staticmethod
    def fn_check_numbers(element,key):
        correct = False
        if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None" and str(element[key]).replace(" ","") != ""):
            if (element[key].replace(",","").replace(".","").isnumeric() == True):
                pass
            else:
                correct = True
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no es numerico,"
        return element

    @staticmethod
    def fn_check_text(element,key):
        if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None" and str(element[key]).replace(" ","") != ""):
            if (str(element[key]).replace(" ","").isalpha() == False):
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no es texto,"
        return element

    @staticmethod
    def fn_check_son_younger_than_parent(element):
        if (element["Parentesco"] is not None  and element["Parentesco"] != "None" and element["Parentesco"] != "null") and str(element["Parentesco"]).replace(" ","") != "" and\
        (element["FechaNacimiento"] is not None  and element["FechaNacimiento"] != "None" and element["FechaNacimiento"] != "null") and str(element["FechaNacimiento"]).replace(" ","") != "" and\
        (element["fechaNacimientoAfiliado"] is not None  and element["fechaNacimientoAfiliado"] != "None" and element["fechaNacimientoAfiliado"] != "null") and str(element["fechaNacimientoAfiliado"]).replace(" ","") != "":
            if  "hijo" in str(element["Parentesco"]).lower():
                if str(element["fechaNacimientoAfiliado"]).strip().replace("-","").replace(",","").replace(".","").replace("/","").isnumeric() and str(element["FechaNacimiento"]).strip().replace("-","").replace(",","").replace(".","").replace("/","").isnumeric():
                    dtmfechanacimiento = float(str(element["fechaNacimientoAfiliado"]).strip().replace("-","").replace(",","").replace(".","").replace("/",""))
                    fechaNacimientoBeneficiario = float(str(element["FechaNacimiento"]).strip().replace("-","").replace(",","").replace(".","").replace("/",""))        
                    if dtmfechanacimiento >= fechaNacimientoBeneficiario:
                        element["validacionDetected"] = element["validacionDetected"] + "edad del beneficiario hijo es mayor o igual a la del afiliado,"
        return element 


    @staticmethod
    def fn_check_pension_survivor_older_25(element):
        if (element["Parentesco"] is not None  and element["Parentesco"] != "None" and element["Parentesco"] != "null") and\
            (element["FechaNacimiento"] is not None  and element["FechaNacimiento"] != "None" and element["FechaNacimiento"] != "null") and\
            (element["CalidadBeneficiario"] is not None  and element["CalidadBeneficiario"] != "None" and element["CalidadBeneficiario"] != "null") and\
            (element["TipoPension"] is not None  and element["TipoPension"] != "None" and element["TipoPension"] != "null"):
            try:
                today = datetime.now()
                born  = datetime.strptime(element["FechaNacimiento"], "%Y%m%d")
                age = today - born
                if element["TipoPension"] == "3":
                    if element["CalidadBeneficiario"] != "I":
                        if (age >= timedelta(days=365*25)):
                            element["validacionDetected"] = element["validacionDetected"] + "pension de sobrevivenvia a mayor de 25 años no invalido,"
                        elif  (age < timedelta(days=365*25) and age >= timedelta(days=365*18)) and (element["Parentesco"].strip() != "ESTUDIANTES ENTRE 18 Y 25 AÑOS"):
                            element["validacionDetected"] = element["validacionDetected"] + "pension de sobrevivenvia a persona entre los 18 y 25 años que no es estudiante,"
            except:
                element["validacionDetected"] = element["validacionDetected"] + "FechaNacimiento tiene un formato de fecha invalida," 
            finally:
                return element
        return element

    @staticmethod
    def fn_check_age_son_ingesting(element):
        if (element["FechaNacimiento"] is not None  and element["FechaNacimiento"] != "None" and element["FechaNacimiento"] != "null") and str(element["FechaNacimiento"]).replace(" ","") != "" and\
           (element["Parentesco"] is not None  and element["Parentesco"] != "None" and element["Parentesco"] != "null") and str(element["Parentesco"]).replace(" ","") != "" and\
           (element["CalidadBeneficiario"] is not None  and element["CalidadBeneficiario"] != "None" and element["CalidadBeneficiario"] != "null") and str(element["Parentesco"]).replace(" ","") != "":
            try:
                today = datetime.now()
                born  = datetime.strptime(element["FechaNacimiento"], "%Y%m%d")
                age = today - born
                if ("hijo" in element["Parentesco"].lower()) or ("estudiante" in element["Parentesco"].lower()):
                    if element["CalidadBeneficiario"] == "S":
                        if (age >= timedelta(days=365*25)):
                            element["validacionDetected"] = element["validacionDetected"] + "persona beneficiaria con parentesco hijo y calidad de beneficiario sano con mas de 25 años al momento del informe,"
            except:
                element["validacionDetected"] = element["validacionDetected"] + "FechaNacimiento tiene un formato de fecha invalida," 
            finally:
                return element
        return element

    @staticmethod
    def fn_check_age_parent_ingesting(element):
        if (element["FechaNacimiento"] is not None  and element["FechaNacimiento"] != "None" and element["FechaNacimiento"] != "null") and str(element["FechaNacimiento"]).replace(" ","") != "" and\
            (element["Parentesco"] is not None  and element["Parentesco"] != "None" and element["Parentesco"] != "null") and str(element["Parentesco"]).replace(" ","") != "":
            try:
                today = datetime.now()
                born  = datetime.strptime(element["FechaNacimiento"], "%Y%m%d")
                age = today - born
                if ("padre" in element["Parentesco"].lower()) or ("madre" in element["Parentesco"].lower()):
                    if (age > timedelta(days=365*95)):
                        element["validacionDetected"] = element["validacionDetected"] + "persona beneficiaria con parentesco madre o padre con mas de 95 años al momento del informe,"              
            except:
                element["validacionDetected"] = element["validacionDetected"] + "FechaNacimiento tiene un formato de fecha invalida,"
            finally:
                return element
        return element

    @staticmethod
    def fn_check_word_lenght(element,key,compareTo):
        if (element[key] is not None  and element[key] != "None" and element[key] != "null" and str(element[key]).replace(" ","") != ""):
            if len(element[key]) < compareTo:
                element["validacionDetected"] = element["validacionDetected"] +key+" tiene un longitud menor de "+str(compareTo)+","
        return element

    @staticmethod
    def fn_check_value_in_bene(element,key,listValues):
        if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None"):
            correct = False
            for value in listValues:
                if str(element[key]).strip() == str(value):
                    correct = True
                    break
            if correct == False:
                element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado en lista,"
        return element


    @staticmethod
    def fn_check_value_in(element,key,listValues):
        if (element[key] is not None  and\
            element[key] != "None" and\
            element[key] != "null"):
            correct = False
            for value in listValues:
                if str(element[key]).strip() == str(value):
                    correct = True
                    break
            if correct == False:
                element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado en lista,"
        return element

    @staticmethod
    def fn_gcpm_case_different_program_retire(element):
        if (element["ModalidadPension"] is not None  and\
            element["ModalidadPension"] != "None" and\
            element["ModalidadPension"] != "null") and\
            element["ModalidadPension"].strip().replace(",","").replace(".","").isnumeric() and\
            element["IndicadorPensionGPM"] is not None  and \
            element["IndicadorPensionGPM"] != "None" and \
            element["IndicadorPensionGPM"] != "null":
            if str(element["IndicadorPensionGPM"]).replace(" ","").lower() == "s":
                if float(str(element["ModalidadPension"]).strip()) not in [float(1),float(2),float(6)]:
                    element["validacionDetected"] = element["validacionDetected"] + "caso GCP con pension diferente a retiro programado,"
        return element

    @staticmethod
    def fn_gpm_less_than_1150(element):
        if (element["IndicadorPensionGPM"] is not None  and \
            element["IndicadorPensionGPM"] != "None" and \
            element["IndicadorPensionGPM"] != "null" and \
            element["TotalSemanas"] is not None and \
            element["TotalSemanas"] != "None" and \
            str(element["TotalSemanas"]).replace(" ","").replace(",","").replace(".","").isnumeric() and \
            element["TotalSemanas"] != "null"):
            if str(element["IndicadorPensionGPM"]).replace(" ","").lower() == "s" and \
            float(str(element["TotalSemanas"]).replace(" ","").replace(",","").replace(".","")) < float(1150):
                element["validacionDetected"] = element["validacionDetected"] + "Pensiones de Garantia de Pension Minima con menos de 1150 semanas,"
        return element


    @staticmethod
    def fn_age_to_fechaSiniestro(element):
        if (element["IndicadorPensionGPM"] is not None  and \
            element["IndicadorPensionGPM"] != "None" and \
            element["IndicadorPensionGPM"] != "null" and \
            element["FechaSiniestro"] is not None  and \
            element["FechaSiniestro"] != "None" and \
            element["FechaSiniestro"] != "null" and \
            element["Sexo"] is not None  and \
            element["Sexo"] != "None" and \
            element["Sexo"] != "null" and \
            element["FechaNacimiento"] is not None  and \
            element["FechaNacimiento"] != "None" and \
            element["FechaNacimiento"] != "null"):
            try:
                fechaNacimiento   = datetime.strptime(element["FechaNacimiento"], "%Y%m%d")
                fechaSiniestro    = datetime.strptime(element["FechaSiniestro"], "%Y%m%d")
                ageToSinisterDate =   fechaSiniestro - fechaNacimiento
                if str(element["IndicadorPensionGPM"]).replace(" ","").lower() == "s":
                    if   (element["Sexo"] == "FEMENINO") and (ageToSinisterDate < timedelta(days=365*57)):
                        element["validacionDetected"] = element["validacionDetected"] + "edad a la fecha de siniestro es inferior a la debida para mujeres  en casos GPM,"                    
                    elif (element["Sexo"] == "MASCULINO") and (ageToSinisterDate < timedelta(days=365*62)):
                        element["validacionDetected"] = element["validacionDetected"] + "edad a la fecha de siniestro es inferior a la debida para hombres en casos GPM,"
            except:
                element["validacionDetected"] = element["validacionDetected"] + "fechaSiniestro o fechaNacimiento no poseen formato AAAMMDD,"
            finally:
                return element                
        return element


    @staticmethod
    def fn_GPM_vejez_SMMLV(element,SMMLV):
        if (element["ValorPago"] is not None  and\
            element["ValorPago"] != "None" and\
            element["ValorPago"] != "null") and\
            element["ValorPago"].strip().replace(",","").replace(".","").isnumeric() and\
            element["IndicadorPensionGPM"] is not None  and \
            element["IndicadorPensionGPM"] != "None" and \
            element["IndicadorPensionGPM"] != "null" and \
            element["tipoDePension"] is not None  and \
            element["tipoDePension"] != "None" and \
            element["tipoDePension"] != "null":
            if str(element["tipoDePension"]).strip() == "1":
                if str(element["IndicadorPensionGPM"]).replace(" ","").lower() == "s":
                    if float(element["ValorPago"]) != float(SMMLV):
                        element["validacionDetected"] = element["validacionDetected"] + "cliente con pension de vejez y caso GPM con pension diferente a un SMMLV,"        
        return element


    @staticmethod
    def fn_caseGPM_saldoAgotado(element):
        if (element["CAI"] is not None  and\
            element["CAI"] != "None" and\
            element["CAI"] != "null") and\
            element["CAI"].strip().replace(",","").replace(".","").isnumeric() and\
            element["IndicadorPensionGPM"] is not None  and \
            element["IndicadorPensionGPM"] != "None" and \
            element["IndicadorPensionGPM"] != "null":
            if float(str(element["CAI"]).strip()) == float(0):
                if str(element["IndicadorPensionGPM"]).replace(" ","").lower() == "s":
                    element["validacionDetected"] = element["validacionDetected"] + "caso GPM con saldo en cero,"        
        return element               


    @staticmethod
    def fn_check_pension_oldeness_women_men(element):
        if (element["tipoDePension"] is not None  and element["tipoDePension"] != "None" and element["tipoDePension"] != "null") and \
           (element["Sexo"] is not None  and element["Sexo"] != "None" and element["Sexo"] != "null") and \
           (element["VejezAnticipada"] is not None  and element["VejezAnticipada"] != "None" and element["VejezAnticipada"] != "null") and \
           (element["FechaNacimiento"] is not None  and element["FechaNacimiento"] != "None" and element["FechaNacimiento"] != "null"):
            try:
                today = datetime.now()
                born  = datetime.strptime(element["FechaNacimiento"], "%Y%m%d")
                age = today - born
                if str(element["tipoDePension"]).strip() == "1":
                    if (element["Sexo"] == "FEMENINO") and (age < timedelta(days=365*57)) and (element["VejezAnticipada"] != "VEJEZ ANTICIPADA"):
                        element["validacionDetected"] = element["validacionDetected"] + "mujer con pension de vejez menor a 57 años de edad sin vejez anticipada,"
                    elif (element["Sexo"] == "MASCULINO") and (age < timedelta(days=365*62)) and (element["VejezAnticipada"] != "VEJEZ ANTICIPADA" ):
                        element["validacionDetected"] = element["validacionDetected"] + "hombre  con pension de vejez menor a 62 años de edad sin vejez anticipada,"                                   
            except:
                element["validacionDetected"] = element["validacionDetected"] + "FechaNacimiento tiene un formato de fecha invalida,"
            finally:
                return element
        return element




    @staticmethod
    def fn_check_pension_oldeness(element):
        if (element["tipoDePension"] is not None  and element["tipoDePension"] != "None" and element["tipoDePension"] != "null") and \
           (element["FechaNacimiento"] is not None  and element["FechaNacimiento"] != "None" and element["FechaNacimiento"] != "null"):
            try:
                today = datetime.now()
                born  = datetime.strptime(element["FechaNacimiento"], "%Y%m%d")
                age = today - born
                if str(element["tipoDePension"]).strip() == "1":
                    if (age > timedelta(days=365*95)):
                        element["validacionDetected"] = element["validacionDetected"] + "pensionado conm edad superior a 95 años,"                
            except:
                element["validacionDetected"] = element["validacionDetected"] + "FechaNacimiento tiene un formato de fecha invalida,"
            finally:
                return element
        return element



    @staticmethod
    def fn_rentavitalicia_pencon_24(element):
        if (element["ModalidadPension"] is not None  and element["ModalidadPension"] != "None" and element["ModalidadPension"] != "null" and element["ModalidadPension"] != "ModalidadPension") and \
           (element["PENCON"] is not None  and element["PENCON"] != "None" and element["PENCON"] != "null" and element["PENCON"] != "PENCON"):
            if str(element["PENCON"]).replace(",","").replace(".","").isnumeric() and str(element["ModalidadPension"]).replace(",","").replace(".","").isnumeric():
                    if int(float(str(element["ModalidadPension"]).strip())) == 2:
                        if int(float(str(element["PENCON"]))) == 24:
                            pass
                        else:
                            element["validacionDetected"] = element["validacionDetected"] + "rentavitalicia con PENCON de la PENARC diferente a 24,"
            else:
                    element["validacionDetected"] = element["validacionDetected"] + "no se puede validar si rentavitalicia tiene PENCON de la PENARC igual a 24 ya que uno de estos no es numerico,"
        return element


    @staticmethod
    def fn_total_weeks_afiliafos_devoluciones(element):
        if (element["TotalSemanas"] is not None  and\
            element["TotalSemanas"] != "None" and\
            element["TotalSemanas"] != "null") and\
            element["TotalSemanas"].strip().replace(",","").replace(".","").isnumeric() and\
            element["afiliadosDevoluciones"] is not None  and \
            element["afiliadosDevoluciones"] != "None" and \
            element["afiliadosDevoluciones"] != "null":
            if float(str(element["TotalSemanas"]).strip()) == float(0):
                element["validacionDetected"] = element["validacionDetected"] + "afiliado con Devoluciones de Saldo con semanas en cero,"
        return element



    @staticmethod
    def fn_total_weeks_afiliafos_devoluciones_vejez(element):
        if (element["TotalSemanas"] is not None  and\
            element["TotalSemanas"] != "None" and\
            element["TotalSemanas"] != "null") and\
            element["TotalSemanas"].strip().replace(",","").replace(".","").isnumeric() and\
            element["afiliadosDevoluciones"] is not None  and \
            element["afiliadosDevoluciones"] != "None" and \
            element["afiliadosDevoluciones"] != "null" and \
            element["tipoDePension"] is not None  and \
            element["tipoDePension"] != "None" and \
            element["tipoDePension"] != "null":
            if element["tipoDePension"] == "1":
                if float(str(element["TotalSemanas"]).strip()) > float(1150):
                    element["validacionDetected"] = element["validacionDetected"] + "riesgo vejez y con un numero de semanas superior a 1.150,"        
        return element



    @staticmethod
    def fn_check_lesser_than(element,key,valueToCompare):
        if (element[key] is not None  and \
            element[key] != "None" and \
            element[key] != "null"):
            if  float(str(element[key]).strip().replace(",","").replace(".","")) > \
                float(str(valueToCompare).strip().replace(",","").replace(".","")):
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" es mayor que "+str(valueToCompare)+","
        return element




    @staticmethod
    def fn_check_afiliado_younger_12(element):
        if (element["FechaNacimiento"] is not None  and element["FechaNacimiento"] != "None" and element["FechaNacimiento"] != "null"):
            try:
                today = datetime.now()
                born  = datetime.strptime(element["FechaNacimiento"], "%Y%m%d")
                age = today - born
                if str(element["tipoDePension"]).strip() == "1":
                    if (age < timedelta(days=365*12)):
                        element["validacionDetected"] = element["validacionDetected"] + "afiliado menor a 12 años,"                
            except:
                element["validacionDetected"] = element["validacionDetected"] + "FechaNacimiento tiene un formato de fecha invalida,"
            finally:
                return element
        return element 


    @staticmethod
    def fn_check_vejezanticipada_solicitud(element):
        if (element["IndicadorPensionGPM"] is not None  and element["IndicadorPensionGPM"] != "None" and element["IndicadorPensionGPM"] != "null") and \
           (element["Sexo"] is not None  and element["Sexo"] != "None" and element["Sexo"] != "null") and \
           (element["VejezAnticipada"] is not None  and element["VejezAnticipada"] != "None" and element["VejezAnticipada"] != "null") and \
           (element["FechaNacimiento"] is not None  and element["FechaNacimiento"] != "None" and element["FechaNacimiento"] != "null") and  \
           (element["FechaSolicitud"] is  not None  and  element["FechaSolicitud"] != "None" and element["FechaSolicitud"] != "null"):
            try:
                fechaNacimiento = datetime.strptime(element["FechaNacimiento"], "%Y%m%d")
                fechaSolicitud = datetime.strptime(element["FechaSolicitud"], "%Y%m%d")
                ageToSolicitud =   fechaSolicitud - fechaNacimiento
                if str(element["IndicadorPensionGPM"]).strip().lower() == "s":
                    if   (element["Sexo"] == "FEMENINO") and (ageToSolicitud < timedelta(days=365*57)):
                        element["validacionDetected"] = element["validacionDetected"] + "edad a la fecha de solicitud es inferior a la debida para mujeres,"                    
                    elif (element["Sexo"] == "MASCULINO") and (ageToSolicitud < timedelta(days=365*62)):
                        element["validacionDetected"] = element["validacionDetected"] + "edad a la fecha de solicitud es inferior a la debida para hombres,"
            except:
                element["validacionDetected"] = element["validacionDetected"] + "FechaNacimiento o FechaSolicitud tiene un formato de fecha invalida,"
            finally:
                return element                
        return element


    @staticmethod
    def fn_check_multimodal_user(element):
        if (str(element["prueba"])) is not None and  \
            str(element["prueba"]) != "None" and  \
            str(element["prueba"]) != "null":
            element["validacionDetected"] = element["validacionDetected"] + "usuario con multiples modalidades de pension,"
        return element



    @staticmethod
    def fn_check_bigger_than_other(element,key1,key2):
        if (element[key1] is not None  and \
            element[key1] != "None" and \
            element[key1] != "null") and \
            element[key1].strip().replace(",","").replace(".","").isnumeric() and \
            element[key2] is not None  and \
            element[key2] != "None" and \
            element[key2] != "null" and \
            element[key2].strip().replace(",","").replace(".","").isnumeric():
            if float(element[key1].strip().replace(",","").replace(".","")) < \
               float(element[key2].strip().replace(",","").replace(".","")):
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key1) +" es menor que "+str(key2)+","
        return element



    @staticmethod
    def fn_devoSaldo_lesser_than(element):
        if (element["CAI"] is not None  and \
            element["CAI"] != "None" and \
            element["CAI"] != "null") and \
            element["CAI"].strip().replace(",","").replace(".","").isnumeric() and \
            element["DevolucionSaldos"] is not None  and \
            element["DevolucionSaldos"] != "None" and \
            element["DevolucionSaldos"] != "null" and \
            element["DevolucionSaldos"].strip().replace(",","").replace(".","").isnumeric() and \
            element["Bono"] is not None  and \
            element["Bono"] != "None" and \
            element["Bono"] != "null" and \
            element["Bono"].strip().replace(",","").replace(".","").isnumeric():
            if float(element["DevolucionSaldos"].strip().replace(",","").replace(".",""))  > \
               (float(element["CAI"].strip().replace(",","").replace(".","")) + \
                float(element["Bono"].strip().replace(",","").replace(".",""))):
                element["validacionDetected"] = element["validacionDetected"] + "devolucion de saldo  es mayor a la suma de bono y saldo CAI,"
        return element



    @staticmethod
    def fn_devoSaldo_lesser_than_bono(element):
        if (element["DevolucionSaldos"] is not None  and \
            element["DevolucionSaldos"] != "None" and \
            element["DevolucionSaldos"] != "null" and \
            element["DevolucionSaldos"].strip().replace(",","").replace(".","").isnumeric() and \
            element["Bono"] is not None  and \
            element["Bono"] != "None" and \
            element["Bono"] != "null" and \
            element["Bono"].strip().replace(",","").replace(".","").isnumeric()):
            if float(element["DevolucionSaldos"].strip().replace(",","").replace(".",""))  < \
               float(element["Bono"].strip().replace(",","").replace(".","")):
                element["validacionDetected"] = element["validacionDetected"] + "devolucion de saldo  es menor al bono,"
        return element


    @staticmethod
    def fn_bono_sin_fecha_reden(element):
        if (element["FechaRedencionBono"] is  None  or \
            element["FechaRedencionBono"] == "None" or \
            element["FechaRedencionBono"] == "null") and \
            element["Bono"] is not None  and \
            element["Bono"] != "None" and \
            element["Bono"] != "null" and \
            element["EstadoBono"] is not None  and \
            element["EstadoBono"] != "None" and \
            element["EstadoBono"] != "null" :
            if str(element["EstadoBono"])  == "EMI" or  \
               str(element["EstadoBono"])  == "RED" or  \
               str(element["EstadoBono"])  == "CUS":
                element["validacionDetected"] = element["validacionDetected"] + "Bono con informacion y estado EMI, RED o CUS sin fecha de rendicion,"
        return element


    @staticmethod
    def fn_check_pension_less_than_50(element):
        if (element["TipoPension"] != "None" and \
            element["TipoPension"] != "null" and \
            element["TipoPension"] is not None and \
            element["SemanasMomentoDefinicion"] != "None" and \
            element["SemanasMomentoDefinicion"] != "null" and \
            str(element["SemanasMomentoDefinicion"]).strip().replace(",","").replace(".","").isnumeric() and \
            element["SemanasMomentoDefinicion"] is not None):
            if element["TipoPension"].strip() == "2" or element["TipoPension"].strip() == "3":
                if float(element["SemanasMomentoDefinicion"].strip().replace(",","").replace(".","")) < float(50):
                    element["validacionDetected"] = element["validacionDetected"] + "persona con pension de invalidez o sobrevivencia con menos de 50 en campo total_semanas - semanasAlMomentoDeLaDefinicion,"
        return element



    @staticmethod
    def fn_check_mesada_pensional_0(element,key):
        if (element[key] is not None  and \
            element[key] != "None" and \
            element[key] != "null") and \
           (element["TipoPersona"] is not None  and \
            element["TipoPersona"] != "None" and \
            element["TipoPersona"] != "null"):
            if element["TipoPersona"] == "Cliente":
                if float(element[key]) <= float(0):
                    element["validacionDetected"] = element["validacionDetected"] + "valorPago es menor o igual a 0,"
        return element


    @staticmethod
    def fn_check_mesada_pensional_below(element,key,SMMLV):
        if (element[key] is not None  and \
            element[key] != "None" and \
            element[key] != "null"):
            if element[key].strip().replace(",","").replace(".","").isnumeric():
                if float(str(element[key]).strip().replace(",","").replace(".","")) < float(SMMLV):
                    element["validacionDetected"] = element["validacionDetected"] + "valor de mesada pensional es menor a el SMMLV,"
        return element


    @staticmethod
    def fn_check_date_compare(element,key1,dateFormat1,key2,dateFormat2):
        if (element[key1] is not None  and \
            element[key1] != "None" and \
            element[key1] != "null") and \
           (element[key2] is not None  and \
            element[key2] != "None"  and \
            element[key2] != "null"): 
            try:
                fechasiniestro  = datetime.strptime(element[key1], dateFormat1)
                fechaNacimiento = datetime.strptime(element[key2], dateFormat2)
                if fechaNacimiento >= fechasiniestro:
                    element["validacionDetected"] = element["validacionDetected"] + key2 + " mayor a " + key1 + ","      
            except:
                element["validacionDetected"] = element["validacionDetected"] + str(key1) +" o "+str(key2)+""+" tiene un formato de fecha invalida," 
            finally:
                return element
        else:
            return element                                                            