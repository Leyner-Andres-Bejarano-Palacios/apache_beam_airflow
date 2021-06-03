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


class ValidadorEntidadEmisorBonoLogic():
    def __init__(self, config):
        self._vName = self.__class__.__name__
        self._vConfig = config

    @staticmethod
    def fn_check_completitud(element,key):
        if (element[key] is None  or str(element[key]).lower() == "none" or (element[key]).lower() == "null" or str(element[key]).replace(" ","") == ""):
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
        return element 



table_schema_dimEntidadEmisorBono = {
    'fields': [{
        'name': 'TipoIdEntidadEmisora', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'EntidadEmisoraBonoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IdEntidadEmisora', 'type':'STRING', 'mode':'NULLABLE'}       
        ]
}

table_schema_dimEntidadEmisorBono_malos = {
    'fields': [{
        'name': 'TipoIdEntidadEmisora', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'EntidadEmisoraBonoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IdEntidadEmisora', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        
        ]
}

config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
options1 = PipelineOptions(
argv= None,
runner=config['configService']['runner'],
project=config['configService']['project'],
job_name='dimentidademisorbono-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimEntidadEmisorBono')


table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimEntidadEmisorBono_dirty')

with beam.Pipeline(options=options1) as p: 

         
    dimEntidadEmisorBono  = (
        
       p | 'Query Table dimEntidadEmisorBono' >> beam.io.ReadFromBigQuery(
            query='''
                    SELECT GENERATE_UUID() as EntidadEmisoraBonoID, TipoIdEntidadEmisora, IdEntidadEmisora FROM (SELECT EZ51.tipoIdEmpleador TipoIdEntidadEmisora, EZ51.idEmpleador IdEntidadEmisora, E.EMP_HASH64
                    FROM '''+config['configService']['datasetDatalake']+'''.AS400_FPOBLIDA_ENTAR9 E INNER JOIN
                    '''+config['configService']['datasetDatalake']+'''.SQLSERVER_BIPROTECCIONDW_DW_DIMEMPLEADORES_Z51 EZ51
                    ON E.EMP_HASH64=EZ51.EMP_HASH64)
                  ''',\
            use_standard_sql=True))


    dimEntidadEmisorBono_Dict = dimEntidadEmisorBono | beam.Map(lambda x: \
                                                            {'TipoIdEntidadEmisora':str(x['IdEntidadEmisora']),\
                                                             'EntidadEmisoraBonoID':str(x['EntidadEmisoraBonoID']),\
                                                             'IdEntidadEmisora':str(x['IdEntidadEmisora'])})





    dimEntidadEmisorBono_Dict |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimEntidadEmisorBono,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)