# -*- coding: utf-8 -*-




import time
import uuid
import types
import threading
import numpy as np
import pandas as pd
import configparser
from datetime import date
import apache_beam as beam
from datetime import datetime
from apache_beam import pvalue
from google.cloud import bigquery as bq
from apache_beam.runners.runner import PipelineState
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions





class fn_divide_clean_dirty(beam.DoFn):
    def process(self, element):
        correct = False
        if element["validacionDetected"] == "":
            correct = True
            del element["validacionDetected"]

        if correct == True:
            yield pvalue.TaggedOutput('Clean', element)
        else:
            yield pvalue.TaggedOutput('validationsDetected', element)


class ValidadorPensionadosLogic():
    def __init__(self, config):
        self._vName = self.__class__.__name__
        self._vConfig = config

    @staticmethod
    def fn_check_completitud(element,key):
        if (element[key] is None  or str(element[key]).lower() == "none" or (element[key]).lower() == "null" or str(element[key]).replace(" ","") == ""):
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
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
    def fn_check_text(element,key):
        if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None" and str(element[key]).replace(" ","") != ""):
            if (str(element[key]).replace(" ","").isalpha() == False):
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no es texto,"
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
    def fn_rentavitalicia_ConsecutivoPension_24(element):
        if (element["ModalidadPension"] is not None  and element["ModalidadPension"] != "None" and element["ModalidadPension"] != "null" and element["ModalidadPension"] != "ModalidadPension") and \
           (element["ConsecutivoPension"] is not None  and element["ConsecutivoPension"] != "None" and element["ConsecutivoPension"] != "null" and element["ConsecutivoPension"] != "ConsecutivoPension"):
            if str(element["ConsecutivoPension"]).replace(",","").replace(".","").isnumeric() and str(element["ModalidadPension"]).replace(",","").replace(".","").isnumeric():
                    if int(float(str(element["ModalidadPension"]).strip())) == 2:
                        if int(float(str(element["ConsecutivoPension"]))) == 24:
                            pass
                        else:
                            element["validacionDetected"] = element["validacionDetected"] + "rentavitalicia con ConsecutivoPension de la PENARC diferente a 24,"
            else:
                    element["validacionDetected"] = element["validacionDetected"] + "no se puede validar si rentavitalicia tiene ConsecutivoPension de la PENARC igual a 24 ya que uno de estos no es numerico,"
        return element





    @staticmethod
    def fn_total_weeks_afiliafos_devoluciones(element):
        if (element["TotalSemanas"] is not None  and\
            element["TotalSemanas"] != "None" and\
            element["TotalSemanas"] != "null") and\
            element["TotalSemanas"].strip().replace(",","").replace(".","").isnumeric() and\
            element["AfiliadoDevolucion"] is not None  and \
            element["AfiliadoDevolucion"] != "None" and \
            element["AfiliadoDevolucion"] != "null":
            if float(str(element["TotalSemanas"]).strip()) == float(0):
                element["validacionDetected"] = element["validacionDetected"] + "afiliado con Devoluciones de Saldo con semanas en cero,"
        return element



    @staticmethod
    def fn_total_weeks_afiliafos_devoluciones_vejez(element):
        if (element["TotalSemanas"] is not None  and\
            element["TotalSemanas"] != "None" and\
            element["TotalSemanas"] != "null") and\
            element["TotalSemanas"].strip().replace(",","").replace(".","").isnumeric() and\
            element["AfiliadoDevolucion"] is not None  and \
            element["AfiliadoDevolucion"] != "None" and \
            element["AfiliadoDevolucion"] != "null" and \
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




table_schema_dimPensionados = {
    'fields': [{
        'name': 'FechaDelDato', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'PensionadosID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ModalidadPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IndicadorPensionGPM', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'NumeroSolicitudPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'tipoDePension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ModalidadInicial', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'afi_hash64', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaNacimiento', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaSiniestro', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ConsecutivoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TotalSemanas', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'AfiliadoDevolucion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'VejezAnticipada', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaSolicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ValorPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CAI', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'proceso_numero_caso', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'Mesadas', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Sexo', 'type':'STRING', 'mode':'NULLABLE'}       
        ]
}

table_schema_dimPensionados_malos = {
    'fields': [{
        'name': 'FechaDelDato', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'PensionadosID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ModalidadPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IndicadorPensionGPM', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'NumeroSolicitudPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'tipoDePension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ModalidadInicial', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'afi_hash64', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaNacimiento', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaSiniestro', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ConsecutivoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TotalSemanas', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'AfiliadoDevolucion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'VejezAnticipada', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaSolicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ValorPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CAI', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Sexo', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'proceso_numero_caso', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'Mesadas', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}




config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
validador = ValidadorPensionadosLogic(config)
options1 = PipelineOptions(
argv= None,
runner=config['configService']['runner'],
project=config['configService']['project'],
job_name='dimpensionados-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimPensionados')


table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimPensionados_dirty')

with beam.Pipeline(options=options1) as p:

           
       
    dimPensionados  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                    WITH pensionados as ((SELECT GENERATE_UUID() AS PensionadosID,
                    CURRENT_DATE() AS FechaDelDato,
                    MODALIDAD AS ModalidadPension,  
                    pen.AFI_HASH64 AS afi_hash64,
                    INDICADOR_GPM AS IndicadorPensionGPM,
                    CAST(NUMERO_SOLICITUD AS STRING) AS NumeroSolicitudPension,
                    TIPO_PENSION AS tipoDePension,
                    pen.ESTADO_PENSION AS EstadoPension,
                    MODALIDAD_INICIAL AS ModalidadInicial,
                    FECHA_PENSION FechaPension,
                    ap.dtmFechaNacimiento AS FechaNacimiento,
                    CAST(NUMERO_MESADAS AS STRING) AS Mesadas,
                    '' Procesos,
                    CAST(FECHA_SINIESTRO AS STRING) AS FechaSiniestro,
                    CONSECUTIVO_PENSION AS ConsecutivoPension,
                    TOTAL_SEMANAS AS TotalSemanas,
                    case when MONTO_DEVOLUCION is not null then 'AfiliadosDevolucion' else 'None' end as AfiliadoDevolucion
                    , case when(pen.CAUSAL='@003' and pen.TIPO_SOLICITUD='VEJ') THEN 'VEJEZ ANTICIPADA' else 'None' END AS VejezAnticipada
                    , FECHA_SOLICITUD AS FechaSolicitud, VALOR_MEDADA_ACTUAL AS ValorPago, CAI, descSexo Sexo
                    FROM '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_pensionadosFutura pen
                    LEFT JOIN 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Apolo ap
                    ON
                    pen.AFI_HASH64 = ap.per_hash64
                    LEFT JOIN 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Advance adv
                    ON 
                    adv.AFI_HASH64=pen.AFI_HASH64
                    LEFT JOIN 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_DMP_CYR_DEVOLUCION_SALDOS desa
                    ON
                    desa.AFI_HASH64=pen.AFI_HASH64)

                    UNION DISTINCT

                    (SELECT GENERATE_UUID() AS PensionadosID,
                    CURRENT_DATE() AS FechaDelDato,
                    'Retiro de saldos' AS ModalidadPension,  
                    re.AFI_HASH64 AS afi_hash64,
                    '' AS IndicadorPensionGPM,
                    CAST(NUMERO_SOLICITUD AS STRING) AS NumeroSolicitudPension,
                    '' AS tipoDePension,
                    '' AS EstadoPension,
                    '' AS ModalidadInicial,
                    '' FechaPension,
                    ap.dtmFechaNacimiento AS FechaNacimiento,
                    '' AS Mesadas,
                    '' Procesos,
                    CAST(FECHA_SINIESTRO AS STRING) AS FechaSiniestro,
                    CAST(CONSECUTIVO_PENSION AS STRING) AS ConsecutivoPension,
                    TOTAL_SEMANAS AS TotalSemanas,
                    case when MONTO_DEVOLUCION is not null then 'AfiliadosDevolucion' else 'None' end as AfiliadoDevolucion
                    ,case when(CAST(re.CAUSAL_SINIESTRO AS STRING)='@003' and re.TIPO_SOLICITUD='VEJ') THEN 'VEJEZ ANTICIPADA' else 'None' END AS VejezAnticipada
                    ,'None' AS FechaSolicitud,
                    MONTO_DEVOLUCION AS ValorPago, CAI, descSexo Sexo
                    FROM '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Retiros re
                    LEFT JOIN 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Apolo ap
                    ON
                    re.AFI_HASH64 = ap.per_hash64
                    LEFT JOIN 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Advance adv
                    ON 
                    adv.AFI_HASH64=re.AFI_HASH64
                    LEFT JOIN 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_DMP_CYR_DEVOLUCION_SALDOS desa
                    ON
                    desa.AFI_HASH64=re.AFI_HASH64
                    )

                    UNION DISTINCT 

                    (
                    SELECT GENERATE_UUID() AS PensionadosID,
                    CURRENT_DATE() AS FechaDelDato,
                    'Rentas vitalicias' AS ModalidadPension,  
                    AFI_HASH64_ AS afi_hash64,
                    '' AS IndicadorPensionGPM,
                    '' AS NumeroSolicitudPension,
                    '' AS tipoDePension,
                    vit.ESTADO_PENSION AS EstadoPension,
                    '' AS ModalidadInicial,
                    CAST(FECHA_GENERACION AS STRING) FechaPension,
                    ap.dtmFechaNacimiento AS FechaNacimiento,
                    '' AS Mesadas,
                    '' Procesos,
                    CAST(vit.FEC_SINIESTRO AS STRING) AS FechaSiniestro,
                    'None' AS ConsecutivoPension,
                    TOTAL_SEMANAS AS TotalSemanas,
                    case when MONTO_DEVOLUCION is not null then 'AfiliadosDevolucion' else 'None' end as AfiliadoDevolucion
                    ,'None' AS VejezAnticipada
                    , 'None' AS FechaSolicitud,
                    MONTO_DEVOLUCION AS ValorPago, CAI, descSexo Sexo
                    FROM '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_DMP_CYR_RENTAS_VITALICIAS vit
                    LEFT JOIN 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Apolo ap
                    ON
                    vit.AFI_HASH64_ = ap.per_hash64
                    LEFT JOIN 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Advance adv
                    ON 
                    adv.AFI_HASH64=vit.AFI_HASH64_
                    LEFT JOIN 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_DMP_CYR_DEVOLUCION_SALDOS desa
                    ON
                    desa.AFI_HASH64=vit.AFI_HASH64_
                    )
                    ), 
                    PRUEBA2 AS (SELECT COUNT(MODALIDAD) AS M1,MODALIDAD, AFI_HASH64
                    FROM `'''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_pensionadosFutura` A
                    GROUP BY
                    MODALIDAD, AFI_HASH64), 
                    PRUEBA3 AS(
                    SELECT CASE WHEN COUNT(M1) >1 THEN 'multipleModalidadAfiliado' else 'None' end as prueba, M1, MODALIDAD, AFI_HASH64
                    FROM PRUEBA2
                    GROUP BY 
                    M1, MODALIDAD, AFI_HASH64),
                    RESULTADO AS(
                    select PensionadosID,
                    FechaDelDato, ModalidadPension, pensionados.afi_hash64,IndicadorPensionGPM,NumeroSolicitudPension,
                    tipoDePension,EstadoPension,ModalidadInicial,FechaPension,FechaNacimiento,Mesadas,ju.NUMERO_DEL_CASO AS proceso_numero_caso,
                    FechaSiniestro, ConsecutivoPension, TotalSemanas, AfiliadoDevolucion, VejezAnticipada, FechaSolicitud, CAI
                    ValorPago, CAI, Sexo
                    from pensionados 
                    LEFT JOIN 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_DMP_CYR_PROCESOS_JUDICIALES ju
                    ON 
                    afi_hash64=AFI_HASH64_)
                    SELECT 
                    PensionadosID,
                    FechaDelDato, ModalidadPension, RESULTADO.afi_hash64,IndicadorPensionGPM,NumeroSolicitudPension,
                    tipoDePension,EstadoPension,ModalidadInicial,FechaPension,FechaNacimiento,Mesadas,proceso_numero_caso,
                    FechaSiniestro, ConsecutivoPension, TotalSemanas, AfiliadoDevolucion, VejezAnticipada, FechaSolicitud, CAI
                    ValorPago, CAI, Sexo, PRUEBA.prueba
                    FROM RESULTADO
                    LEFT JOIN
                    (SELECT DISTINCT AFI_hash64, prueba FROM 
                    PRUEBA3) PRUEBA
                    ON 
                    PRUEBA.AFI_HASH64=RESULTADO.AFI_HASH64

                ''',\
            use_standard_sql=True))




    dimPensionados_Dict = dimPensionados | beam.Map(lambda x: \
                                                            {'FechaDelDato':str(x['FechaDelDato']),\
                                                            'PensionadosID':str(x['PensionadosID']),\
                                                            'ModalidadPension':str(x['ModalidadPension']),\
                                                            'IndicadorPensionGPM':str(x['IndicadorPensionGPM']),\
                                                            'NumeroSolicitudPension':str(x['NumeroSolicitudPension']),\
                                                            'tipoDePension':str(x['tipoDePension']),\
                                                            'EstadoPension':str(x['EstadoPension']),\
                                                            'ModalidadInicial':str(x['ModalidadInicial']),\
                                                            'afi_hash64':str(x['afi_hash64']),\
                                                            'FechaPension':str(x['FechaPension']),\
                                                            'FechaNacimiento':str(x['FechaNacimiento']),\
                                                            'FechaSiniestro':str(x['FechaSiniestro']),\
                                                            'ConsecutivoPension':str(x['ConsecutivoPension']),\
                                                            'TotalSemanas':str(x['TotalSemanas']),\
                                                            'AfiliadoDevolucion':str(x['AfiliadoDevolucion']),\
                                                            'VejezAnticipada':str(x['VejezAnticipada']),\
                                                            'FechaSolicitud':str(x['FechaSolicitud']),\
                                                            'ValorPago':str(x['ValorPago']),\
                                                            'CAI':str(x['CAI']),\
                                                            'Sexo':str(x['Sexo']),\
                                                            'prueba':str(x['prueba']),\
                                                            'proceso_numero_caso':str(x['proceso_numero_caso']),\
                                                            'Mesadas':str(x['Mesadas']),\
                                                             'validacionDetected':""})
                                                             # FechaSiniestro
                                                             # ConsecutivoPension
                                                             # TotalSemanas
                                                             # AfiliadoDevolucion
                                                             # VejezAnticipada
                                                             # FechaSolicitud
                                                             # ValorPago
                                                             # CAI



    #. validaciones  identificacion

    # Identificacion_fullness_validated = dimPensionados_Dict | 'completitud identificacion' >> beam.Map(fn_check_completitud,    'DocumentoDeLaPersona' )

    # Identificacion_numbers_validated = Identificacion_fullness_validated | 'solo numeros identificacion' >> beam.Map( fn_check_numbers,  'DocumentoDeLaPersona')

    # tipoDocumentoDeLaPersona_validated = Identificacion_numbers_validated | 'completitud tipoDocumentoDeLaPersona' >> beam.Map(fn_check_completitud,    'tipoDocumentoDeLaPersona' )

    # IdentificacionBeneficiario_qualityRule30_newNumeration = tipoDocumentoDeLaPersona_validated | 'repeated identification with different identification document type' >> beam.Map(fn_repeatedId_different_docType)

    #. validaciones ModalidadInicial

    ModalidadInicial_fullness_validated = dimPensionados_Dict | 'completitud ModalidadInicial' >> beam.Map(validador.fn_check_completitud,   'ModalidadInicial' )


    #. fecha pension

    FechaPension_fullness_validated = ModalidadInicial_fullness_validated | 'completitud FechaPension' >> beam.Map(validador.fn_check_completitud,   'FechaPension')



    #. Fecha Siniestro


    #FechaSiniestro_QualityRule_8_checked = FechaSiniestro_fullness_validated | 'regla 8 validada, FechaSiniestro  debe ser menor a fecha nacimiento ' >> beam.Map(fn_check_completitud,   'FechaSiniestro')

    #. Estado Pension

    EstadoPension_fullness_validated = FechaPension_fullness_validated | 'completitud EstadoPension' >> beam.Map(validador.fn_check_completitud,   'EstadoPension')

    #validaciones modalidaPension     

    ModalidadPension_fullness_validated = EstadoPension_fullness_validated | 'completitud modalidaPension' >> beam.Map(validador.fn_check_completitud,   'ModalidadPension')

    ModalidadPension_conformidad_validated = ModalidadPension_fullness_validated | 'conformidad, debe ser uno de los 7 valores' >> beam.Map(validador.fn_check_value_in,   'ModalidadPension', ["1","2","3","4","5","6","7"])

    ModalidadPension_RetiroProgramado_casoGPM =  ModalidadPension_conformidad_validated | 'Los casos GPM deben tener tipo de pensión retiro programado' >> beam.Map(validador.fn_gcpm_case_different_program_retire)

    #. Indicador Pension GPM

    IndicadorPensionGPM_fullness_validated = ModalidadPension_RetiroProgramado_casoGPM | 'completitud IndicadorPensionGPM' >> beam.Map(validador.fn_check_completitud,   'IndicadorPensionGPM')

    IndicadorPensionGPM_less_than_1150_weeks = IndicadorPensionGPM_fullness_validated | 'Pensiones de Garantia de Pension Minima y 1150 semanas' >>  beam.Map(validador.fn_gpm_less_than_1150)

    IndicadorPensionGPM_age_to_fechaSiniestro = IndicadorPensionGPM_less_than_1150_weeks | 'edad 62 en hombre, 57 en mujeres al momento de la fecha de siniestro para casos GPM' >>  beam.Map(validador.fn_age_to_fechaSiniestro)

    IndicadorPensionGPM_valorPension_qualityRule80 = IndicadorPensionGPM_age_to_fechaSiniestro  | 'Vejez GPM must be one SMLV' >>  beam.Map(validador.fn_GPM_vejez_SMMLV, config['DEFAULT']['SMMLV'])

    IndicadorPensionGPM_saldoAgotado = IndicadorPensionGPM_valorPension_qualityRule80  | 'Vejez GPM saldo agotado' >>  beam.Map(validador.fn_caseGPM_saldoAgotado)

    #, Mesadas

    Mesadas_fullness_validated = IndicadorPensionGPM_saldoAgotado | 'completitud Mesadas' >> beam.Map(validador.fn_check_completitud,   'Mesadas')

    Mesadas_onlyNumbers_validated = Mesadas_fullness_validated | 'solo numeros Mesadas' >> beam.Map(validador.fn_check_numbers,   'Mesadas')

    Mesadas_valueInList_validated = Mesadas_onlyNumbers_validated | 'value in Mesadas' >> beam.Map(validador.fn_check_value_in,   'Mesadas',["12.0","13.0","14.0","12","13","14"]) 


    #. FechaNacimiento

    FechaNacimiento_fullness_validated = Mesadas_valueInList_validated | 'completitud FechaNacimiento' >> beam.Map(validador.fn_check_completitud,   'FechaNacimiento')

    FechaNacimiento_QualityRule_10_checked = FechaNacimiento_fullness_validated | 'pension before 62 men 57 women' >> beam.Map(validador.fn_check_pension_oldeness_women_men)

    FechaNacimiento_QualityRule_46_checked = FechaNacimiento_QualityRule_10_checked | 'pension oldeness after 95' >> beam.Map(validador.fn_check_pension_oldeness)

    # validaciones sexo

    Sexo_fullness_validated = FechaNacimiento_QualityRule_46_checked | 'completitud sexo' >> beam.Map( validador.fn_check_completitud,  'Sexo')

    Sexo_text_validated = Sexo_fullness_validated | 'solo texto sexo' >> beam.Map( validador.fn_check_text, 'Sexo')

    # validaciones Nombre

    # Nombre_fullness_validated = Sexo_text_validated | 'completitud Nombre' >> beam.Map(validador.fn_check_completitud,   'Nombre')

    FechaDelDato_fullness_validated = Sexo_text_validated | 'completitud FechaDelDato' >> beam.Map(validador.fn_check_completitud,   'FechaDelDato')

    PensionadosID_fullness_validated = FechaDelDato_fullness_validated | 'completitud PensionadosID' >> beam.Map(validador.fn_check_completitud,   'PensionadosID')

    NumeroSolicitudPension_fullness_validated = PensionadosID_fullness_validated | 'completitud NumeroSolicitudPension' >> beam.Map(validador.fn_check_completitud,   'NumeroSolicitudPension')

    tipoDePension_fullness_validated = NumeroSolicitudPension_fullness_validated | 'completitud tipoDePension' >> beam.Map(validador.fn_check_completitud,   'tipoDePension')

    FechaPension_dateFormat_validated = tipoDePension_fullness_validated | 'date format FechaPension' >> beam.Map(validador.fn_check_date_format,    'FechaPension', "%Y%m%d" )

    FechaPension_biggerThan_validated = FechaPension_dateFormat_validated | 'bigger than FechaPension' >> beam.Map(validador.fn_check_bigger_than,    'FechaPension', 19940401)

    RentaVitalicia_ConsecutivoPension_VALIDATED = FechaPension_biggerThan_validated | 'rentaVitalicia, es decir modalidad pension de pension 2 debe ser ConsecutivoPension 24'  >> beam.Map(validador.fn_rentavitalicia_ConsecutivoPension_24)

    afiliadoDevolucion_totalSemanas = RentaVitalicia_ConsecutivoPension_VALIDATED | 'afiliado con Devoluciones de Saldo no debe tener semanas en cero'  >> beam.Map(validador.fn_total_weeks_afiliafos_devoluciones)

    afiliadoDevolucion_totalSemanas_vejez = afiliadoDevolucion_totalSemanas | 'afiliado con Devoluciones de Saldo no debe tener semanas en cero, vejez'  >> beam.Map(validador.fn_total_weeks_afiliafos_devoluciones_vejez)

    Pensionado_con_SaldoCai = afiliadoDevolucion_totalSemanas_vejez  | 'pensionado con Saldo CAI mayor a 0'  >> beam.Map(validador.fn_check_lesser_than, 'CAI',0)

    twekveYeasr_validated = Pensionado_con_SaldoCai  | 'menor de 12 annos' >> beam.Map(validador.fn_check_afiliado_younger_12)

    # tipoIdentificacion_validated =  twekveYeasr_validated | 'identificacion TI o RCN con edad igual o mayor a 18 años' >> beam.Map(fn_check_tipo_identificacion_age)

    # identificacionIncosistente_validated =  tipoIdentificacion_validated |  'identificacion inconsistente' >> beam.Map(fn_check_ID_PAS)

    # identificacionItInconsistente = identificacionIncosistente_validated |  'identificacion TI inconsistente' >> beam.Map(fn_check_ID_TI)

    # coincidenciaEntre_validated = identificacionItInconsistente   |  'coincidencia entre nombres' >> beam.Map(fn_check_nombre_others)

    # Nombre_solo_texto = coincidenciaEntre_validated | 'Nombres solo pueden tener caracteres alfanumericos'  >> beam.Map(fn_check_text,'Nombre')

    VejezAnticipadaSolicitud_validation = twekveYeasr_validated  | 'vejez anticipada con solicitud de pension'  >>  beam.Map(validador.fn_check_vejezanticipada_solicitud)

    multipensionmodality_validation = VejezAnticipadaSolicitud_validation | 'usuario con multiples modalidades de pension'  >>  beam.Map(validador.fn_check_multimodal_user)

    # longitudCC_validation = multipensionmodality_validation  | 'usuario con CC de longitud 9 o 11'  >>  beam.Map(fn_check_CC)

    # longitudIdPEP_validation = longitudCC_validation  | 'usuario con tipo de identificacion PEP de longitud  diferente a 7 o 15'  >>  beam.Map(fn_check_PEP)


    # FechaNacimiento_o_QualityRule_64_checked = 


    # fn_check_pension_tipoPension_2

    



    results = multipensionmodality_validation | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x: \
                                                            {'FechaDelDato':str(x['FechaDelDato']),\
                                                            'PensionadosID':str(x['PensionadosID']),\
                                                            'ModalidadPension':str(x['ModalidadPension']),\
                                                            'IndicadorPensionGPM':str(x['IndicadorPensionGPM']),\
                                                            'NumeroSolicitudPension':str(x['NumeroSolicitudPension']),\
                                                            'tipoDePension':str(x['tipoDePension']),\
                                                            'EstadoPension':str(x['EstadoPension']),\
                                                            'ModalidadInicial':str(x['ModalidadInicial']),\
                                                            'afi_hash64':str(x['afi_hash64']),\
                                                            'FechaPension':str(x['FechaPension']),\
                                                            'FechaNacimiento':str(x['FechaNacimiento']),\
                                                            'FechaSiniestro':str(x['FechaSiniestro']),\
                                                            'ConsecutivoPension':str(x['ConsecutivoPension']),\
                                                            'TotalSemanas':str(x['TotalSemanas']),\
                                                            'AfiliadoDevolucion':str(x['AfiliadoDevolucion']),\
                                                            'VejezAnticipada':str(x['VejezAnticipada']),\
                                                            'FechaSolicitud':str(x['FechaSolicitud']),\
                                                            'ValorPago':str(x['ValorPago']),\
                                                            'CAI':str(x['CAI']),\
                                                            'proceso_numero_caso':str(x['proceso_numero_caso']),\
                                                            'Mesadas':str(x['Mesadas'])})



    dirty_ones = results["validationsDetected"] |  beam.Map(lambda x: \
                                                            {'FechaDelDato':str(x['FechaDelDato']),\
                                                            'PensionadosID':str(x['PensionadosID']),\
                                                            'ModalidadPension':str(x['ModalidadPension']),\
                                                            'IndicadorPensionGPM':str(x['IndicadorPensionGPM']),\
                                                            'NumeroSolicitudPension':str(x['NumeroSolicitudPension']),\
                                                            'tipoDePension':str(x['tipoDePension']),\
                                                            'EstadoPension':str(x['EstadoPension']),\
                                                            'ModalidadInicial':str(x['ModalidadInicial']),\
                                                            'afi_hash64':str(x['afi_hash64']),\
                                                            'FechaPension':str(x['FechaPension']),\
                                                            'FechaNacimiento':str(x['FechaNacimiento']),\
                                                            'FechaSiniestro':str(x['FechaSiniestro']),\
                                                            'ConsecutivoPension':str(x['ConsecutivoPension']),\
                                                            'TotalSemanas':str(x['TotalSemanas']),\
                                                            'AfiliadoDevolucion':str(x['AfiliadoDevolucion']),\
                                                            'VejezAnticipada':str(x['VejezAnticipada']),\
                                                            'FechaSolicitud':str(x['FechaSolicitud']),\
                                                            'ValorPago':str(x['ValorPago']),\
                                                            'CAI':str(x['CAI']),\
                                                            'proceso_numero_caso':str(x['proceso_numero_caso']),\
                                                            'Mesadas':str(x['Mesadas']),\
                                                            'validacionDetected':str(x['validacionDetected'])})


    dirty_ones | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimPensionados_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimPensionados,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            


