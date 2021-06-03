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


class ValidadorfactDefinicionesLogic():
    def __init__(self, config):
        self._vName = self.__class__.__name__
        self._vConfig = config

    @staticmethod
    def fn_check_completitud(element,key):
        if (element[key] is None  or str(element[key]).lower() == "none" or (element[key]).lower() == "null" or str(element[key]).replace(" ","") == ""):
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
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
    def fn_check_lesser_than(element,key,valueToCompare):
        if (element[key] is not None  and \
            element[key] != "None" and \
            element[key] != "null"):
            if  float(str(element[key]).strip().replace(",","").replace(".","")) > \
                float(str(valueToCompare).strip().replace(",","").replace(".","")):
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" es mayor que "+str(valueToCompare)+","
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


table_schema_factDefiniciones = {
    'fields': [
        {
        'name':'DefinicionesID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Inf_personasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPensionID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasMomentoDefinicion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasProteccion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasAFP', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasBono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IBLUltimosDiezAnnos', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IBLTodaLaVida', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDefinicion', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_factDefiniciones_malos = {
    'fields': [
        {
        'name':'DefinicionesID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Inf_personasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPensionID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasMomentoDefinicion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasProteccion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasAFP', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasBono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IBLUltimosDiezAnnos', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IBLTodaLaVida', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDefinicion', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}


config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
validador = ValidadorfactDefinicionesLogic(config)
options1 = PipelineOptions(
argv= None,
runner=config['configService']['runner'],
project=config['configService']['project'],
job_name='factdefiniciones-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_factDefiniciones')


table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_factDefiniciones_dirty')

with beam.Pipeline(options=options1) as p:     
        
    factDefiniciones  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                        SELECT GENERATE_UUID() AS DefinicionesID, 'None' as TiempoID, CURRENT_DATE() AS FechaDato, 
                        'None' as Inf_personasID, 'None' as TipoPensionID,
                        TOTAL_SEMANAS AS SemanasMomentoDefinicion,
                        SEMANAS_PROTECCION as SemanasProteccion,
                        SEMANAS_OTRAS_AFP as SemanasAFP,
                        SEMANAS_BONO1 AS SemanasBono,
                        IBL1 IBLUltimosDiezAnnos,
                        IBL2 IBLTodaLaVida,
                        FECHA_CORTE_IBL1 as FechaDefinicion
                        FROM `'''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Advance`
                                ''',\
            use_standard_sql=True))






    factDefiniciones_Dict = factDefiniciones | beam.Map(lambda x: \
                                                            {'DefinicionesID':str(x['DefinicionesID']),\
                                                            'TiempoID':str(x['TiempoID']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'Inf_personasID':str(x['Inf_personasID']),\
                                                            'TipoPensionID':str(x['TipoPensionID']),\
                                                            'SemanasMomentoDefinicion':str(x['SemanasMomentoDefinicion']),\
                                                            'SemanasProteccion':str(x['SemanasProteccion']),\
                                                            'SemanasAFP':str(x['SemanasAFP']),\
                                                            'SemanasBono':str(x['SemanasBono']),\
                                                            'IBLUltimosDiezAnnos':str(x['IBLUltimosDiezAnnos']),\
                                                            'IBLTodaLaVida':str(x['IBLTodaLaVida']),\
                                                            'FechaDefinicion':str(x['FechaDefinicion']),\
                                                            'validacionDetected':""})



    SemanasMomentoDefinicion_fullness_validated = factDefiniciones_Dict | 'completitud SemanasMomentoDefinicion' >> beam.Map(validador.fn_check_completitud,    'SemanasMomentoDefinicion' )

    SemanasMomentoDefinicion_biggerThan_validated = SemanasMomentoDefinicion_fullness_validated | 'bigger than SemanasMomentoDefinicion' >> beam.Map(validador.fn_check_bigger_than,    'SemanasMomentoDefinicion',0 )

    SemanasMomentoDefinicion_QualityRule_58 = SemanasMomentoDefinicion_biggerThan_validated | 'smaller than SemanasMomentoDefinicion' >> beam.Map(validador.fn_check_lesser_than,    'SemanasMomentoDefinicion',1150 )  

    SemanasBono_validated = SemanasMomentoDefinicion_QualityRule_58 | 'completitud SemanasBono' >> beam.Map(validador.fn_check_completitud,    'SemanasBono' )

    SemanasAFP_validated = SemanasBono_validated | 'completitud SemanasAFP' >> beam.Map(validador.fn_check_completitud,    'SemanasAFP' )

    SemanasProteccion_validated = SemanasAFP_validated | 'completitud SemanasProteccion' >> beam.Map(validador.fn_check_completitud,    'SemanasProteccion' )

    #****************** SemanasMomentoDefinicio_QualityRule_19 = SemanasProteccion_validated | 'pension invalidez or survivor with less than 50 total weeks' >> beam.Map(validador.fn_check_pension_less_than_50) 

    










#     Sexo_MoF_validated = Sexo_text_validated | 'sexo en valores' >> beam.Map( fn_check_value_in_bene('Sexo',["M","F"]))



#     FechaNacimiento_fullness_validated = Nombre_fullness_validated | 'completitud FechaNacimiento' >> beam.Map(fn_check_completitud('FechaNacimiento'))

#     FechaNacimiento_biggerThan120_validated = FechaNacimiento_fullness_validated | 'nas de 120 annos FechaNacimiento' >> beam.Map( fn_age_less_120('FechaNacimiento'))


#     IdentificacionPension_fullness_validated = FechaNacimiento_biggerThan120_validated | 'completitud IdentificacionPension' >> beam.Map( fn_check_completitud_bene('IdentificacionPension'))

#     IdentificacionPension_numbers_validated = IdentificacionPension_fullness_validated | 'numeros IdentificacionPension' >> beam.Map( fn_check_numbers('IdentificacionPension'))

#     estadoBeneficiario_fullness_validated = IdentificacionPension_numbers_validated | 'completitud estadoBeneficiario' >> beam.Map( fn_check_completitud_bene('estadoBeneficiario'))

#     estadoBeneficiario_text_validated = estadoBeneficiario_fullness_validated | 'solo texto estadoBeneficiario' >> beam.Map( fn_check_text('estadoBeneficiario'))

#     estadoBeneficiario_IoS_validated = Sexo_text_validated | 'estadoBeneficiario en valores' >> beam.Map( fn_check_value_in_bene('estadoBeneficiario',["I","S"]))

#     calidadBeneficiario_completitud_validated = Sexo_text_validated | 'calidadBeneficiario completitud' >> beam.Map( fn_check_completitud_bene('calidadBeneficiario'))

#     TipoBeneficiario_completitud_validated = Sexo_text_validated | 'TipoBeneficiario completitud' >> beam.Map( fn_check_completitud_bene('TipoBeneficiario'))

#     SubtipoBeneficiario_completitud_validated = Sexo_text_validated | 'SubtipoBeneficiario completitud' >> beam.Map( fn_check_completitud_bene('SubtipoBeneficiario'))

#     ParentescoBeneficiario_completitud_validated = Sexo_text_validated | 'ParentescoBeneficiario completitud' >> beam.Map( fn_check_completitud_bene('ParentescoBeneficiario'))

#     ParentescoBeneficiario_text_validated = ParentescoBeneficiario_completitud_validated | 'ParentescoBeneficiario texto' >> beam.Map( fn_check_text('ParentescoBeneficiario'))
    



#    #  regla 22 

#    BeneficiarioMenorAfiliado_text_validated = ParentescoBeneficiario_text_validated | 'Beneficiario menor a afiliado' >> beam.Map( fn_bene_younger_cliient())

#    BeneficiarioMayorAfiliado_text_validated = BeneficiarioMenorAfiliado_text_validated | 'Beneficiario mayor a afiliado' >> beam.Map( fn_bene_older_cliient())

#     #  regla 30

#    numeroRepeticiones_text_validated = BeneficiarioMayorAfiliado_text_validated | 'dos o mas Beneficiarios con misma Id' >> beam.Map( fn_repetead_id("numeroRepeticiones",1))

#    nombreBene_text_validated = numeroRepeticiones_text_validated | 'nombre o apellido de mas de dos caracteres' >> beam.Map( fn_bene_short_name("Nombre",1))

#    EstadoAfiliadoFutura_EstadoAfiliadoApolo_match = nombreBene_text_validated | 'match entre EstadoAfiliadoFutura y EstadoAfiliadoApolo' >> beam.Map( fn_fimd_no_mathcing("EstadoAfiliadoFutura","EstadoAfiliadoApolo"))






    



    results = SemanasProteccion_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x: \
                                                            {'DefinicionesID':str(x['DefinicionesID']),\
                                                            'TiempoID':str(x['TiempoID']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'Inf_personasID':str(x['Inf_personasID']),\
                                                            'TipoPensionID':str(x['TipoPensionID']),\
                                                            'SemanasMomentoDefinicion':str(x['SemanasMomentoDefinicion']),\
                                                            'SemanasProteccion':str(x['SemanasProteccion']),\
                                                            'SemanasAFP':str(x['SemanasAFP']),\
                                                            'SemanasBono':str(x['SemanasBono']),\
                                                            'IBLUltimosDiezAnnos':str(x['IBLUltimosDiezAnnos']),\
                                                            'IBLTodaLaVida':str(x['IBLTodaLaVida']),\
                                                            'FechaDefinicion':str(x['FechaDefinicion'])})                                                                                     





    dirty = results["validationsDetected"] | beam.Map(lambda x: \
                                                            {'DefinicionesID':str(x['DefinicionesID']),\
                                                            'TiempoID':str(x['TiempoID']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'Inf_personasID':str(x['Inf_personasID']),\
                                                            'TipoPensionID':str(x['TipoPensionID']),\
                                                            'SemanasMomentoDefinicion':str(x['SemanasMomentoDefinicion']),\
                                                            'SemanasProteccion':str(x['SemanasProteccion']),\
                                                            'SemanasAFP':str(x['SemanasAFP']),\
                                                            'SemanasBono':str(x['SemanasBono']),\
                                                            'IBLUltimosDiezAnnos':str(x['IBLUltimosDiezAnnos']),\
                                                            'IBLTodaLaVida':str(x['IBLTodaLaVida']),\
                                                            'FechaDefinicion':str(x['FechaDefinicion']),\
                                                            'validacionDetected':str(x['validacionDetected'])})







    dirty | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_factDefiniciones_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_factDefiniciones,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


