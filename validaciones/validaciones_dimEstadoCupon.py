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


class ValidadorEstadoCuponLogic():
    def __init__(self, config):
        self._vName = self.__class__.__name__
        self._vConfig = config

    @staticmethod
    def fn_check_completitud(element,key):
        if (element[key] is None  or str(element[key]).lower() == "none" or (element[key]).lower() == "null" or str(element[key]).replace(" ","") == ""):
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
        return element 

  
table_schema_dimEstadoCupon = {
    'fields': [{
        'name': 'EstadoCuponID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'EstadoCupon', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimEstadoCupon_malos = {
    'fields': [{
        'name': 'EstadoCuponID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'EstadoCupon', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'},
        ]
}


config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
validador = ValidadorEstadoCuponLogic(config)
options1 = PipelineOptions(
    argv= None,
    runner=config['configService']['runner'],
    project=config['configService']['project'],
    job_name='dimestadocupon-apache-beam-job-name',
    temp_location=config['configService']['temp_location'],
    region=config['configService']['region'],
    service_account_email=config['configService']['service_account_email'],
    save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimEstadoCupon')


table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimEstadoCupon_dirty')

with beam.Pipeline(options=options1) as p:

                
       
    dimEstadoCupon  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                select GENERATE_UUID() as EstadoCuponID, EstadoCupon from(SELECT distinct
                ESTADO_CUPON as EstadoCupon
                FROM
                '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_SUPERCUPON)
                ''',\
            use_standard_sql=True))





    dimEstadoCupon_Dict = dimEstadoCupon | beam.Map(lambda x: \
                                                            {'EstadoCuponID':str(x['EstadoCuponID']),\
                                                             'EstadoCupon':str(x['EstadoCupon']),\
                                                             'validacionDetected':""})


    EstadoCuponID_fullness_validated = dimEstadoCupon_Dict | 'completitud EstadoCuponID' >> beam.Map(validador.fn_check_completitud,    'EstadoCuponID' )

    EstadoCupon_fullness_validated = EstadoCuponID_fullness_validated | 'completitud EstadoCupon' >> beam.Map(validador.fn_check_completitud,    'EstadoCupon' )

    



    results = EstadoCupon_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x:  {'EstadoCuponID':str(x['EstadoCuponID']).encode(encoding = 'utf-8'),\
                                                                      'EstadoCupon':str(x['EstadoCupon']).encode(encoding = 'utf-8')})




    results["validationsDetected"]  | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimEstadoCupon_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimEstadoCupon,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)




