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


class ValidadorBeneficiariosLogic():
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




table_schema_dimBeneficiarios = {
    'fields': [{
        'name': 'BeneficiariosID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'afi_hash64', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'fechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaFinTemporalidad', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ConsecutivoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SecuenciaBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CalidadBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaNacimiento', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SubtipoBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
         'name':'IdentificacionPension'  , 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Parentesco', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'fechaNacimientoAfiliado', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPension', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimBeneficiarios_malos = {
    'fields': [{
        'name': 'BeneficiariosID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'afi_hash64', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'fechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaFinTemporalidad', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ConsecutivoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SecuenciaBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CalidadBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaNacimiento', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SubtipoBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
         'name':'IdentificacionPension'  , 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Parentesco', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'fechaNacimientoAfiliado', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPension', 'type':'STRING', 'mode':'NULLABLE'},          
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}


config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
validador = ValidadorBeneficiariosLogic(config)
options1 = PipelineOptions(
argv= None,
runner=config['configService']['runner'],
project=config['configService']['project'],
job_name='dimbeneficiarios-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimBeneficiarios')


table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimBeneficiarios_dirty')

with beam.Pipeline(options=options1) as p:
    dimBeneficiarios  = (
        p
        | 'Query Table Beneficiarios' >> beam.io.ReadFromBigQuery(
            query='''
                    WITH BENEFICIARIOS AS (SELECT GENERATE_UUID() AS BeneficiariosID, 
                    CURRENT_DATE() AS fechaDato, AFI_HASH64 as afi_hash64,
                    cast(FECHA_FIN_TEMPORALIDAD as string) as FechaFinTemporalidad,
                    cast(CONSECUTIVO_PENSION as string) ConsecutivoPension,
                    SECUENCIA_BENEFICIARIO SecuenciaBeneficiario,
                    CALIDAD_BENEFICIARIO CalidadBeneficiario,
                    FECHA_NACIMIENTO_BENEFICIARIO FechaNacimiento,
                    null as IdentificacionPension, 
                    TIPO_BENEFICIARIO TipoBeneficiario,
                    SUBTIPO_BENEFICIARIO SubtipoBeneficiario,
                    PARENTESCO Parentesco, tipo_pension
                    from '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_pensionadosFutura
                    UNION DISTINCT 
                    SELECT GENERATE_UUID() AS BeneficiariosID, 
                    CURRENT_DATE() AS fechaDato, 
                    AFI_HASH64 as afi_hash64,
                    '' as FechaFinTemporalidad,
                    cast(CONSECUTIVO_PENSION as string) ConsecutivoPension,
                    cast(SECUENCIA_BENEFICIARIO as string) SecuenciaBeneficiario,
                    cast(CALIDAD_BENEFICIARIO as string) CalidadBeneficiario,
                    cast(FECHA_NACIMIENTO_BENEFICIARIO as string) FechaNacimiento,
                    null as IdentificacionPension, 
                    TIPO_BENEFICIARIO TipoBeneficiario,
                    cast(SUBTIPO_BENEFICIARIO as string) SubtipoBeneficiario,
                    '' Parentesco, 'None'tipo_pension
                    from '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Retiros
                    UNION DISTINCT 
                    SELECT GENERATE_UUID() AS BeneficiariosID, 
                    CURRENT_DATE() AS fechaDato, 
                    AFI_HASH64 as afi_hash64,
                    '' as FechaFinTemporalidad,
                    cast(CONSECUTIVO_PENSION as string) ConsecutivoPension,
                    '' SecuenciaBeneficiario,
                    cast(CALIDAD_BENEFICIARIO as string) CalidadBeneficiario,
                    cast(FECHA_NACIMIENTO_BENEFICIARIO as string) FechaNacimiento,
                    null as IdentificacionPension, 
                    '' TipoBeneficiario,
                    '' SubtipoBeneficiario,
                    '' Parentesco, CAST(tipo_pension AS STRING)
                    from '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_pensionadosSantander)

                    SELECT BeneficiariosID,fechaDato,
                    afi_hash64, FechaFinTemporalidad, ConsecutivoPension, SecuenciaBeneficiario
                    ,CalidadBeneficiario,  FechaNacimiento,
                    IdentificacionPension, TipoBeneficiario, Parentesco, SubtipoBeneficiario, ap.dtmfechaNacimiento as fechaNacimientoAfiliado, TIPO_PENSION AS TipoPension FROM BENEFICIARIOS 
                    LEFT JOIN 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Apolo ap
                    ON
                    ap.per_hash64=BENEFICIARIOS.AFI_HASH64
                ''',\
            use_standard_sql=True))





    dimBeneficiarios_Dict = dimBeneficiarios | beam.Map(lambda x: \
                                                            {'BeneficiariosID':str(x['BeneficiariosID']),\
                                                                'afi_hash64':str(x['afi_hash64']),\
                                                                'fechaDato':str(x['fechaDato']),\
                                                                'FechaFinTemporalidad':str(x['FechaFinTemporalidad']),\
                                                                'ConsecutivoPension':str(x['ConsecutivoPension']),\
                                                                'SecuenciaBeneficiario':str(x['SecuenciaBeneficiario']),\
                                                                'CalidadBeneficiario':str(x['CalidadBeneficiario']),\
                                                                'FechaNacimiento':str(x['FechaNacimiento']),\
                                                                'TipoBeneficiario':str(x['TipoBeneficiario']),\
                                                                'SubtipoBeneficiario':str(x['SubtipoBeneficiario']),\
                                                                'IdentificacionPension':str(x['IdentificacionPension']),\
                                                                'fechaNacimientoAfiliado':str(x['fechaNacimientoAfiliado']),\
                                                                'Parentesco':str(x['Parentesco']),\
                                                                'TipoPension':str(x['TipoPension']),\
                                                                'validacionDetected':""})



    # TipoIdentificacionBeneficiario_fullness_validated = dimBeneficiarios_Dict | 'completitud TipoIdentificacionBeneficiario' >> beam.Map(validador.fn_check_completitud,    'TipoIdentificacionBeneficiario' )

    # TipoIdentificacionBeneficiario_text_validated = TipoIdentificacionBeneficiario_fullness_validated | 'solo texto TipoIdentificacionBeneficiario' >> beam.Map( validador.fn_check_text,  'TipoIdentificacionBeneficiario')

    FechasNacimiento_fullness_validated = dimBeneficiarios_Dict | 'completitud FechasNacimiento' >> beam.Map(validador.fn_check_completitud,    'FechaNacimiento' )

    FechasNacimiento_dateFormat_validated = FechasNacimiento_fullness_validated | 'date format FechasNacimiento' >> beam.Map(validador.fn_check_date_format,    'FechaNacimiento', "%Y%m%d" )

    # IdentificacionBeneficiario_fullness_validated = FechasNacimiento_dateFormat_validated | 'completitud IdentificacionBeneficiario' >> beam.Map(validador.fn_check_completitud,    'IdentificacionBeneficiario' )

    # IdentificacionBeneficiario_numbers_validated = IdentificacionBeneficiario_fullness_validated | 'solo numeros IdentificacionBeneficiario' >> beam.Map( validador.fn_check_numbers,  'IdentificacionBeneficiario')

    # IdentificacionBeneficiario_biggerThan_validated = IdentificacionBeneficiario_numbers_validated | 'bigger than IdentificacionBeneficiario' >> beam.Map(validador.fn_check_bigger_than,    'IdentificacionBeneficiario',0 )

    # IdentificacionBeneficiario_qualityRule30_newNumeration = IdentificacionBeneficiario_biggerThan_validated | 'repeated identification with different identification document type' >> beam.Map(validador.fn_repeatedId_different_docType)

    fechaFinTemporalidad_dateFormat_validated = FechasNacimiento_dateFormat_validated | 'date format FechaFinTemporalidad' >> beam.Map(validador.fn_check_date_format,    'FechaFinTemporalidad', "%Y%m%d" )

    fechaFinTemporalidad_biggerThan_validated = fechaFinTemporalidad_dateFormat_validated | 'bigger than FechaFinTemporalidad' >> beam.Map(validador.fn_check_bigger_than,    'FechaFinTemporalidad', 20131231)

    ConsecutivoPension_fullness_validated = fechaFinTemporalidad_biggerThan_validated | 'completitud ConsecutivoPension' >> beam.Map(validador.fn_check_completitud,    'ConsecutivoPension' )

    ConsecutivoPension_numbers_validated = ConsecutivoPension_fullness_validated | 'solo numeros ConsecutivoPension' >> beam.Map( validador.fn_check_numbers,  'ConsecutivoPension')

    SecuenciaBeneficiario_fullness_validated = ConsecutivoPension_numbers_validated | 'completitud SecuenciaBeneficiario' >> beam.Map(validador.fn_check_completitud,    'SecuenciaBeneficiario' )

    SecuenciaBeneficiario_numbers_validated = SecuenciaBeneficiario_fullness_validated | 'solo numeros SecuenciaBeneficiario' >> beam.Map( validador.fn_check_numbers,  'SecuenciaBeneficiario')

    CalidadBeneficiario_fullness_validated = SecuenciaBeneficiario_numbers_validated | 'completitud CalidadBeneficiario' >> beam.Map(validador.fn_check_completitud,    'CalidadBeneficiario' )

    TipoBeneficiario_fullness_validated = CalidadBeneficiario_fullness_validated | 'completitud TipoBeneficiario' >> beam.Map(validador.fn_check_completitud,    'TipoBeneficiario' )

    SubtipoBeneficiario_fullness_validated = TipoBeneficiario_fullness_validated | 'completitud SubtipoBeneficiario' >> beam.Map(validador.fn_check_completitud,    'SubtipoBeneficiario' )

    Parentesco_fullness_validated = SubtipoBeneficiario_fullness_validated | 'completitud Parentesco' >> beam.Map(validador.fn_check_completitud,    'Parentesco' )

    Son_youngerThan_parents = Parentesco_fullness_validated | 'son younger than parents' >> beam.Map(validador.fn_check_son_younger_than_parent)

    TipoPension_QualityRule_6_checked = Son_youngerThan_parents | 'there can not be survivor money for helthy people older than 25' >> beam.Map(validador.fn_check_pension_survivor_older_25)

    FechasNacimiento_QualityRule_25 = TipoPension_QualityRule_6_checked | 'there can not be son with healthy quality older than 25' >> beam.Map(validador.fn_check_age_son_ingesting)

    FechasNacimiento_QualityRule_26 = FechasNacimiento_QualityRule_25 | 'there can not be parents older than 95' >> beam.Map(validador.fn_check_age_parent_ingesting)

    BeneficiariosId_fullness_validated = FechasNacimiento_QualityRule_26 | 'completitud BeneficiariosId' >> beam.Map(validador.fn_check_completitud,    'BeneficiariosID' )

    FechaDato_fullness_validated = BeneficiariosId_fullness_validated | 'completitud FechaDato' >> beam.Map(validador.fn_check_completitud,    'fechaDato' )

    # PrimerNombre_fullness_validated = FechaDato_fullness_validated | 'completitud primerNombre' >> beam.Map(validador.fn_check_completitud,    'primerNombre' )

    # primerApellido_fullness_validated = PrimerNombre_fullness_validated | 'completitud primerApellido' >> beam.Map(validador.fn_check_completitud,    'primerApellido' )

    # Nombre_text_validated = primerApellido_fullness_validated | 'solo texto Nombre' >> beam.Map( validador.fn_check_text,  'Nombre')

    # PrimerNombre_QualityRule_31 = Nombre_text_validated  | 'solo texto primerNombre' >> beam.Map( validador.fn_check_word_lenght,  'primerNombre', 2)

    # PrimerApellido_QualityRule_31 = PrimerNombre_QualityRule_31  | 'solo texto primerApellido' >> beam.Map( validador.fn_check_word_lenght,  'primerApellido', 2)

    # namesCoincidence =  PrimerApellido_QualityRule_31  | 'coincidencia primernombre y primerapellido' >> beam.Map(validador.fn_value_equalitty, "primerNombre", "primerApellido")

    # namesCoincidence2 =  namesCoincidence  | 'coincidencia primernombre y segundoApellido' >> beam.Map(validador.fn_value_equalitty, "primerNombre", "segundoApellido")

    # namesCoincidence3 =  namesCoincidence2  | 'coincidencia segundoNombre y segundoApellido' >> beam.Map(validador.fn_value_equalitty, "segundoNombre", "segundoApellido")

    # namesCoincidence4 =  namesCoincidence3  | 'coincidencia segundoNombre y primerapellido' >> beam.Map(validador.fn_value_equalitty, "segundoNombre", "primerApellido")

    # namesCoincidence5 =  namesCoincidence4  | 'coincidencia primernombre y segundoNombre' >> beam.Map(validador.fn_value_equalitty, "primerNombre", "segundoNombre")




    results = FechaDato_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()

    limpias = results["Clean"] | beam.Map(lambda x:   {'BeneficiariosID':str(x['BeneficiariosID']),\
                                                        'afi_hash64':str(x['afi_hash64']),\
                                                        'fechaDato':str(x['fechaDato']),\
                                                        'FechaFinTemporalidad':str(x['FechaFinTemporalidad']),\
                                                        'ConsecutivoPension':str(x['ConsecutivoPension']),\
                                                        'SecuenciaBeneficiario':str(x['SecuenciaBeneficiario']),\
                                                        'CalidadBeneficiario':str(x['CalidadBeneficiario']),\
                                                        'FechaNacimiento':str(x['FechaNacimiento']),\
                                                        'TipoBeneficiario':str(x['TipoBeneficiario']),\
                                                        'SubtipoBeneficiario':str(x['SubtipoBeneficiario']),\
                                                        'fechaNacimientoAfiliado':str(x['fechaNacimientoAfiliado']),\
                                                        'IdentificacionPension':str(x['IdentificacionPension']),\
                                                        'TipoPension':str(x['TipoPension']),\
                                                        'Parentesco':str(x['Parentesco'])})




    dirty = results["validationsDetected"] | beam.Map(lambda x:   {'BeneficiariosID':str(x['BeneficiariosID']),\
                                                        'afi_hash64':str(x['afi_hash64']),\
                                                        'fechaDato':str(x['fechaDato']),\
                                                        'FechaFinTemporalidad':str(x['FechaFinTemporalidad']),\
                                                        'ConsecutivoPension':str(x['ConsecutivoPension']),\
                                                        'SecuenciaBeneficiario':str(x['SecuenciaBeneficiario']),\
                                                        'CalidadBeneficiario':str(x['CalidadBeneficiario']),\
                                                        'FechaNacimiento':str(x['FechaNacimiento']),\
                                                        'TipoBeneficiario':str(x['TipoBeneficiario']),\
                                                        'SubtipoBeneficiario':str(x['SubtipoBeneficiario']),\
                                                        'fechaNacimientoAfiliado':str(x['fechaNacimientoAfiliado']),\
                                                        'IdentificacionPension':str(x['IdentificacionPension']),\
                                                        'TipoPension':str(x['TipoPension']),\
                                                        'Parentesco':str(x['Parentesco']),\
                                                        'validacionDetected':str(x['validacionDetected'])})
                                                            




    dirty | "write dirty ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimBeneficiarios_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimBeneficiarios,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
