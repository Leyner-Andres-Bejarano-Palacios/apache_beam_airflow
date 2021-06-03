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


class ValidadorCuponBonoLogic():
    def __init__(self, config):
        self._vName = self.__class__.__name__
        self._vConfig = config

    @staticmethod
    def fn_check_completitud(element,key):
        if (element[key] is None  or str(element[key]).lower() == "none" or (element[key]).lower() == "null" or str(element[key]).replace(" ","") == ""):
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
        return element 



  
table_schema_dimCuponesBono = {
    'fields': [{
        'name': 'CuponesBonoID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'ConsecutivoCupon', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoCuponID', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimCuponesBono_malos = {
    'fields': [{
        'name': 'CuponesBonoID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'ConsecutivoCupon', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoCuponID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}



config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
validador = ValidadorCuponBonoLogic(config)
options1 = PipelineOptions(
argv= None,
runner=config['configService']['runner'],
project=config['configService']['project'],
job_name='dimcuponesbono-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimCuponesBono')


table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimCuponesBono_dirty')

with beam.Pipeline(options=options1) as p:     

       
    dimInfPersonas  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                    select GENERATE_UUID() as CuponesBonoID, ConsecutivoCupon, EstadoCuponID FROM
                    (SELECT distinct a.CONSECUTIVO_CUPON as ConsecutivoCupon, b.EstadoCuponID
                    FROM `'''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_SUPERCUPON` a
                    LEFT JOIN
                    `'''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_dimEstadoCupon` b
                    ON
                    a.ESTADO_CUPON=b.EstadoCupon)
                ''',\
            use_standard_sql=True))






    dimInfPersonas_Dict = dimInfPersonas | beam.Map(lambda x:   {'CuponesBonoID':str(x['CuponesBonoID']),\
                                                                 'ConsecutivoCupon':str(x['ConsecutivoCupon']),\
                                                                 'EstadoCuponID':str(x['EstadoCuponID']),\
                                                                 'validacionDetected':""})



    CuponesBonoID_fullness_validated = dimInfPersonas_Dict | 'completitud CuponesBonoID' >> beam.Map(validador.fn_check_completitud,    'CuponesBonoID' )

    ConsecutivoCupon_fullness_validated = CuponesBonoID_fullness_validated | 'completitud ConsecutivoCupon' >> beam.Map(validador.fn_check_completitud,    'ConsecutivoCupon' )

    EstadoCuponID_fullness_validated = ConsecutivoCupon_fullness_validated | 'completitud EstadoCuponID' >> beam.Map(validador.fn_check_completitud,    'EstadoCuponID' )





    results = EstadoCuponID_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x:   {'CuponesBonoID':str(x['CuponesBonoID']),\
                                                       'ConsecutivoCupon':str(x['ConsecutivoCupon']),\
                                                       'EstadoCuponID':str(x['EstadoCuponID'])})








    results["validationsDetected"] | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimCuponesBono_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimCuponesBono,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
