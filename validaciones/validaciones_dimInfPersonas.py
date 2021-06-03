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


class ValidadorInfPersonasLogic():
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
    def fn_check_word_lenght(element,key,compareTo):
        if (element[key] is not None  and element[key] != "None" and element[key] != "null" and str(element[key]).replace(" ","") != ""):
            if len(element[key]) < compareTo:
                element["validacionDetected"] = element["validacionDetected"] +key+" tiene un longitud menor de "+str(compareTo)+","
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




table_schema_dimInfPersonas = {
    'fields': [{
        'name': 'InfPersonasID', 'type': 'STRING', 'mode': 'NULLABLE'     
    },{
        'name':'PensionadosID', 'type':'STRING', 'mode':'NULLABLE'},      
        {
        'name':'BeneficiariosID', 'type':'STRING', 'mode':'NULLABLE'},    
        {
        'name':'TipoDePersona', 'type':'STRING', 'mode':'NULLABLE'},      
        {
        'name':'FechaAfiliacion', 'type':'STRING', 'mode':'NULLABLE'},     
        {
        'name':'EstadoCivil', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'sexo', 'type':'STRING', 'mode':'NULLABLE'},               
        {
        'name':'cartera', 'type':'STRING', 'mode':'NULLABLE'},                          
        {
        'name':'NitEmpleador', 'type':'STRING', 'mode':'NULLABLE'},       
        {
        'name':'TipoVinculacion', 'type':'STRING', 'mode':'NULLABLE'},    
        {
        'name':'DependienteIndependiente', 'type':'STRING', 'mode':'NULLABLE'},                      
        {
        'name':'UltimoEmpleadorAfiliado', 'type':'STRING', 'mode':'NULLABLE'},           
        {
        'name':'UltimaOcupacionAfiliado', 'type':'STRING', 'mode':'NULLABLE'}             
        ]
}





table_schema_dimInfPersonas_malos = {
    'fields': [{
        'name': 'InfPersonasID', 'type': 'STRING', 'mode': 'NULLABLE'     
    },{
        'name':'PensionadosID', 'type':'STRING', 'mode':'NULLABLE'},      
        {
        'name':'BeneficiariosID', 'type':'STRING', 'mode':'NULLABLE'},    
        {
        'name':'TipoDePersona', 'type':'STRING', 'mode':'NULLABLE'},      
        {
        'name':'FechaAfiliacion', 'type':'STRING', 'mode':'NULLABLE'},     
        {
        'name':'EstadoCivil', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'sexo', 'type':'STRING', 'mode':'NULLABLE'},               
        {
        'name':'cartera', 'type':'STRING', 'mode':'NULLABLE'},                          
        {
        'name':'NitEmpleador', 'type':'STRING', 'mode':'NULLABLE'},       
        {
        'name':'TipoVinculacion', 'type':'STRING', 'mode':'NULLABLE'},    
        {
        'name':'DependienteIndependiente', 'type':'STRING', 'mode':'NULLABLE'},                      
        {
        'name':'UltimoEmpleadorAfiliado', 'type':'STRING', 'mode':'NULLABLE'},           
        {
        'name':'UltimaOcupacionAfiliado', 'type':'STRING', 'mode':'NULLABLE'},   
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}                
        ]
}

config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
validador = ValidadorInfPersonasLogic(config)
options1 = PipelineOptions(
argv= None,
runner=config['configService']['runner'],
project=config['configService']['project'],
job_name='diminfpersonas-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimInfPersonas')


table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimInfPersonas_dirty')

with beam.Pipeline(options=options1) as p:

            
      
    dimInfPersonas  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                        WITH afiliados AS (SELECT GENERATE_UUID() AS InfPersonasID,
                        'None' AS BeneficiariosID,
                        pen.PensionadosID AS PensionadosID,
                        'Afiliado' TipoDePersona,
                        'None' as FechaAfiliacion, --este campo es el mismo de fecha de pensiòn?
                        apr.descEstadoCivil as EstadoCivil,
                        ap.descsexo sexo,
                        'None' cartera, -- de donde sale este campo?
                        'None' UltimaOcupacionAfiliado, -- de donde sale este campo?
                        'None' UltimoEmpleadorAfiliado, -- de donde sale este campo?
                        'None' NitEmpleador, -- no sabmos
                        'None' TipoVinculacion, -- no sabemos de donde sale este campo
                        'None' DependienteIndependiente -- como sabemos esto?
                        FROM '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Apolo ap
                        LEFT JOIN 
                        '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_ApoloReserva apr
                        ON
                        ap.per_hash64 =apr.per_hash64_
                        LEFT JOIN
                        '''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_dimPensionados pen
                        ON
                        pen.afi_hash64=ap.per_hash64),

                        beneficiarios AS (
                        SELECT GENERATE_UUID() AS InfPersonasID,
                        ben.BeneficiariosID AS BeneficiariosID,
                        'None' AS PensionadosID,
                        'Beneficiario' TipoDePersona,
                        'None' as FechaAfiliacion, --este campo es el mismo de fecha de pensiòn?
                        descEstadoCivil EstadoCivil,
                        ap.descsexo sexo,
                        'None' cartera, -- de donde sale este campo?
                        'None' UltimaOcupacionAfiliado, -- de donde sale este campo?
                        'None' UltimoEmpleadorAfiliado, -- de donde sale este campo?
                        'None' NitEmpleador, -- no sabmos
                        'None' TipoVinculacion, -- no sabemos de donde sale este campo
                        'None' DependienteIndependiente -- como sabemos esto? 
                        FROM 
                        '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Apolo ap    
                        LEFT JOIN
                        '''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_dimBeneficiarios ben
                        ON
                        ben.afi_hash64=ap.per_hash64
                        LEFT JOIN 
                        '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_ApoloReserva apr
                        ON
                        ap.per_hash64 =apr.per_hash64_)

                        SELECT * FROM 
                        afiliados
                        UNION ALL 
                        select * from
                        beneficiarios

                ''',\
            use_standard_sql=True))




    dimInfPersonas_Dict = dimInfPersonas | beam.Map(lambda x: \
                                                            {'InfPersonasID':str(x['InfPersonasID']),\
                                                             'PensionadosID':str(x['PensionadosID']),\
                                                             'BeneficiariosID':str(x['BeneficiariosID']),\
                                                             'TipoDePersona':str(x['TipoDePersona']),\
                                                             'EstadoCivil':str(x['EstadoCivil']),\
                                                             'cartera':str(x['cartera']),\
                                                             'sexo':str(x['sexo']),\
                                                             'NitEmpleador':str(x['NitEmpleador']),\
                                                             'TipoVinculacion':str(x['TipoVinculacion']),\
                                                             'DependienteIndependiente':str(x['DependienteIndependiente']),\
                                                             'UltimoEmpleadorAfiliado':str(x['UltimoEmpleadorAfiliado']),\
                                                             'UltimaOcupacionAfiliado':str(x['UltimaOcupacionAfiliado']),\
                                                             'FechaAfiliacion':str(x['FechaAfiliacion']),\
                                                             'validacionDetected':""})





    FechaAfiliacion_fullness_validated = dimInfPersonas_Dict | 'completitud FechaAfiliacion' >> beam.Map(validador.fn_check_completitud,   'FechaAfiliacion')

    FechaAfiliacion_dateFormat_validated = FechaAfiliacion_fullness_validated | 'date format FechaAfiliacion' >> beam.Map(validador.fn_check_date_format,    'FechaAfiliacion', "%Y%m%d" )

    FechaAfiliacion_biggerThan_validated = FechaAfiliacion_dateFormat_validated | 'bigger than FechaAfiliacion' >> beam.Map(validador.fn_check_bigger_than,    'FechaAfiliacion', 19940401)    

    EstadoCivil_fullness_validated = FechaAfiliacion_biggerThan_validated | 'completitud EstadoCivil' >> beam.Map(validador.fn_check_completitud,   'EstadoCivil')

    UltimoEmpleadorAfiliado_fullness_validated = EstadoCivil_fullness_validated | 'completitud UltimoEmpleadorAfiliado' >> beam.Map(validador.fn_check_completitud,   'UltimoEmpleadorAfiliado')

    UltimoEmpleadorAfiliado_lenght_validated = UltimoEmpleadorAfiliado_fullness_validated | 'lenght UltimoEmpleadorAfiliado' >> beam.Map(validador.fn_check_word_lenght,   'UltimoEmpleadorAfiliado', 9)

    UltimoEmpleadorAfiliado_numbers_validated = UltimoEmpleadorAfiliado_lenght_validated  | 'numbera UltimoEmpleadorAfiliado' >> beam.Map(validador.fn_check_numbers,   'UltimoEmpleadorAfiliado')

    NitEmpleador_fullness_validated = UltimoEmpleadorAfiliado_numbers_validated  | 'completitud NitEmpleador' >> beam.Map(validador.fn_check_completitud,   'NitEmpleador')

    NitEmpleador_numbers_validated = NitEmpleador_fullness_validated   | 'numbers NitEmpleador' >> beam.Map(validador.fn_check_numbers,   'NitEmpleador')

    NitEmpleador_lenght_validated = NitEmpleador_numbers_validated | 'lenght NitEmpleador' >> beam.Map(validador.fn_check_word_lenght,   'NitEmpleador', 9)

    TipoVinculacion_fullness_validated = NitEmpleador_lenght_validated | 'completitud TipoVinculacion' >> beam.Map(validador.fn_check_completitud,   'TipoVinculacion')

    sexo_fullness_validated = TipoVinculacion_fullness_validated | 'completitud sexo' >> beam.Map(validador.fn_check_completitud,    'sexo' )

    sexo_text_validated = sexo_fullness_validated | 'solo texto sexo' >> beam.Map( validador.fn_check_text, 'sexo')

    sexo_MoF_validated = sexo_text_validated | 'sexo en valores' >> beam.Map( validador.fn_check_value_in_bene,'sexo',["M","F","m","f"])

#     sexo_MoF_validated = sexo_text_validated | 'sexo en valores' >> beam.Map( fn_check_value_in_bene('sexo',["M","F"]))



#     FechaNacimiento_fullness_validated = Nombre_fullness_validated | 'completitud FechaNacimiento' >> beam.Map(fn_check_completitud('FechaNacimiento'))

#     FechaNacimiento_biggerThan120_validated = FechaNacimiento_fullness_validated | 'nas de 120 annos FechaNacimiento' >> beam.Map( fn_age_less_120('FechaNacimiento'))


#     IdentificacionPension_fullness_validated = FechaNacimiento_biggerThan120_validated | 'completitud IdentificacionPension' >> beam.Map( fn_check_completitud_bene('IdentificacionPension'))

#     IdentificacionPension_numbers_validated = IdentificacionPension_fullness_validated | 'numeros IdentificacionPension' >> beam.Map( fn_check_numbers('IdentificacionPension'))

#     estadoBeneficiario_fullness_validated = IdentificacionPension_numbers_validated | 'completitud estadoBeneficiario' >> beam.Map( fn_check_completitud_bene('estadoBeneficiario'))

#     estadoBeneficiario_text_validated = estadoBeneficiario_fullness_validated | 'solo texto estadoBeneficiario' >> beam.Map( fn_check_text('estadoBeneficiario'))

#     estadoBeneficiario_IoS_validated = sexo_text_validated | 'estadoBeneficiario en valores' >> beam.Map( fn_check_value_in_bene('estadoBeneficiario',["I","S"]))

#     calidadBeneficiario_completitud_validated = sexo_text_validated | 'calidadBeneficiario completitud' >> beam.Map( fn_check_completitud_bene('calidadBeneficiario'))

#     TipoBeneficiario_completitud_validated = sexo_text_validated | 'TipoBeneficiario completitud' >> beam.Map( fn_check_completitud_bene('TipoBeneficiario'))

#     SubtipoBeneficiario_completitud_validated = sexo_text_validated | 'SubtipoBeneficiario completitud' >> beam.Map( fn_check_completitud_bene('SubtipoBeneficiario'))

#     ParentescoBeneficiario_completitud_validated = sexo_text_validated | 'ParentescoBeneficiario completitud' >> beam.Map( fn_check_completitud_bene('ParentescoBeneficiario'))

#     ParentescoBeneficiario_text_validated = ParentescoBeneficiario_completitud_validated | 'ParentescoBeneficiario texto' >> beam.Map( fn_check_text('ParentescoBeneficiario'))
    



#    #  regla 22 

#    BeneficiarioMenorAfiliado_text_validated = ParentescoBeneficiario_text_validated | 'Beneficiario menor a afiliado' >> beam.Map( fn_bene_younger_cliient())

#    BeneficiarioMayorAfiliado_text_validated = BeneficiarioMenorAfiliado_text_validated | 'Beneficiario mayor a afiliado' >> beam.Map( fn_bene_older_cliient())

#     #  regla 30

#    numeroRepeticiones_text_validated = BeneficiarioMayorAfiliado_text_validated | 'dos o mas Beneficiarios con misma Id' >> beam.Map( fn_repetead_id("numeroRepeticiones",1))

#    nombreBene_text_validated = numeroRepeticiones_text_validated | 'nombre o apellido de mas de dos caracteres' >> beam.Map( fn_bene_short_name("Nombre",1))

#    EstadoAfiliadoFutura_EstadoAfiliadoApolo_match = nombreBene_text_validated | 'match entre EstadoAfiliadoFutura y EstadoAfiliadoApolo' >> beam.Map( fn_fimd_no_mathcing("EstadoAfiliadoFutura","EstadoAfiliadoApolo"))






    



    results = sexo_MoF_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x: \
                                                            {'InfPersonasID':str(x['InfPersonasID']),\
                                                             'PensionadosID':str(x['PensionadosID']),\
                                                             'BeneficiariosID':str(x['BeneficiariosID']),\
                                                             'TipoDePersona':str(x['TipoDePersona']),\
                                                             'EstadoCivil':str(x['EstadoCivil']),\
                                                             'cartera':str(x['cartera']),\
                                                             'sexo':str(x['sexo']),\
                                                             'NitEmpleador':str(x['NitEmpleador']),\
                                                             'TipoVinculacion':str(x['TipoVinculacion']),\
                                                             'DependienteIndependiente':str(x['DependienteIndependiente']),\
                                                             'UltimoEmpleadorAfiliado':str(x['UltimoEmpleadorAfiliado']),\
                                                             'UltimaOcupacionAfiliado':str(x['UltimaOcupacionAfiliado']),\
                                                             'FechaAfiliacion':str(x['FechaAfiliacion'])})


    dirty_ones = results["validationsDetected"] | beam.Map(lambda x: \
                                                            {'InfPersonasID':str(x['InfPersonasID']),\
                                                             'PensionadosID':str(x['PensionadosID']),\
                                                             'BeneficiariosID':str(x['BeneficiariosID']),\
                                                             'TipoDePersona':str(x['TipoDePersona']),\
                                                             'EstadoCivil':str(x['EstadoCivil']),\
                                                             'cartera':str(x['cartera']),\
                                                             'sexo':str(x['sexo']),\
                                                             'NitEmpleador':str(x['NitEmpleador']),\
                                                             'TipoVinculacion':str(x['TipoVinculacion']),\
                                                             'DependienteIndependiente':str(x['DependienteIndependiente']),\
                                                             'UltimoEmpleadorAfiliado':str(x['UltimoEmpleadorAfiliado']),\
                                                             'UltimaOcupacionAfiliado':str(x['UltimaOcupacionAfiliado']),\
                                                             'FechaAfiliacion':str(x['FechaAfiliacion']),\
                                                             'validacionDetected':str(x['validacionDetected'])})



    # validadas = results["validationsDetected"] | beam.Map(lambda x: \
    #                                                     {'InformacionPersonasId':str(x['InformacionPersonasId']).encode(encoding = 'utf-8'),\
    #                                                     'FechaCarga':str(x['FechaCarga']).encode(encoding = 'utf-8'),\
    #                                                         'Identificacion':str(x['Identificacion']).encode(encoding = 'utf-8'),\
    #                                                         'tipoIdentificacion':str(x['tipoIdentificacion']).encode(encoding = 'utf-8'),\
    #                                                         'IdentificacionPension':str(x['IdentificacionPension']).encode(encoding = 'utf-8'),\
    #                                                         'sexo':str(x['sexo']).encode(encoding = 'utf-8'),\
    #                                                         'Nombre':str(x['Nombre']).encode(encoding = 'utf-8'),\
    #                                                         'estadoBeneficiario':str(x['estadoBeneficiario']).encode(encoding = 'utf-8'),\
    #                                                         'consecutivoPension':str(x['consecutivoPension']).encode(encoding = 'utf-8'),\
    #                                                         'FechaNacimiento':str(x['FechaNacimiento']).encode(encoding = 'utf-8'),\
    #                                                         'SecuenciaBeneficiario':str(x['SecuenciaBeneficiario']).encode(encoding = 'utf-8'),\
    #                                                         'TipoPersona':str(x['TipoPersona']).encode(encoding = 'utf-8'),\
    #                                                         'calidadBeneficiario':str(x['calidadBeneficiario']).encode(encoding = 'utf-8'),\
    #                                                         'TipoBeneficiario':str(x['TipoBeneficiario']).encode(encoding = 'utf-8'),\
    #                                                         'SubtipoBeneficiario':str(x['SubtipoBeneficiario']).encode(encoding = 'utf-8'),\
    #                                                         'fechaVigenciaInicial':str(x['fechaVigenciaInicial']).encode(encoding = 'utf-8'),\
    #                                                         'fechaVigenciaFinal':str(x['fechaVigenciaFinal']).encode(encoding = 'utf-8'),\
    #                                                         'validacionDetected':x['validacionDetected']})







    dirty_ones | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimInfPersonas_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimInfPersonas,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


