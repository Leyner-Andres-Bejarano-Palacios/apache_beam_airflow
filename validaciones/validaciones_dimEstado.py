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


class ValidadorEstadoLogic():
    def __init__(self, config):
        self._vName = self.__class__.__name__
        self._vConfig = config

    @staticmethod
    def fn_check_completitud(element,key):
        if (element[key] is None  or str(element[key]).lower() == "none" or (element[key]).lower() == "null" or str(element[key]).replace(" ","") == ""):
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
        return element 




  
table_schema_dimEstado = {
    'fields': [{
        'name': 'EstadoID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'Estado', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Atributo', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Descripcion', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimEstado_malos = {
    'fields': [{
        'name': 'EstadoID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'Estado', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Atributo', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Descripcion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        
        ]
}


config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
validador = ValidadorEstadoLogic(config)
options1 = PipelineOptions(
argv= None,
runner=config['configService']['runner'],
project=config['configService']['project'],
job_name='dimestado-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimEstado')


table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimEstado_dirty')
     
with beam.Pipeline(options=options1) as p:
   
       
    dimEstado  = (
        p
        | 'Query Table dimEstado' >> beam.io.ReadFromBigQuery(
            query='''
                    SELECT GENERATE_UUID() as EstadoID, Estado, Descripcion, Atributo FROM
                    (SELECT distinct CODIGO_ESTADO as Estado, DESCRIPCION_ESTADO 
                    AS Descripcion , "Pension" Atributo
                    FROM `'''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_ESTADO_PENSION`
                    UNION ALL
                    SELECT distinct CODIGO_ESTADO as EstadoD,DESCRIPCION_ESTADO as Descripcion, 
                    "Solicitud" Atributo
                    FROM `'''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_ESTADO_SOLICITUD`)
                ''',\
            use_standard_sql=True))




    dimEstado_Dict = dimEstado | beam.Map(lambda x: \
                                                            {'EstadoID':str(x['EstadoID']),\
                                                             'Estado':str(x['Estado']),\
                                                             'Descripcion':str(x['Descripcion']),\
                                                             'Atributo':str(x['Atributo']),\
                                                             'validacionDetected':""})




    EstadoSolicitud_fullness_validated = dimEstado_Dict | 'completitud EstadoSolicitud' >> beam.Map(validador.fn_check_completitud,    'Estado' )
    #. validaciones  EstadoSolicitudID
    Atributo_fullness_validated = dimEstado_Dict | 'completitud Atributo' >> beam.Map(validador.fn_check_completitud,    'Atributo' )

    results = Atributo_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()

    limpias = results["Clean"] | beam.Map(lambda x: \
                                                            {'EstadoID':str(x['EstadoID']),\
                                                             'Estado':str(x['Estado']),\
                                                             'Descripcion':str(x['Descripcion']),\
                                                             'Atributo':str(x['Atributo'])})



    results["validationsDetected"] | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimEstado_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimEstado,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
