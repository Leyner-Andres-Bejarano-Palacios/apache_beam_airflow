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


class ValidadorEstadoPagoLogic():
    def __init__(self, config):
        self._vName = self.__class__.__name__
        self._vConfig = config

    @staticmethod
    def fn_check_completitud(element,key):
        if (element[key] is None  or str(element[key]).lower() == "none" or (element[key]).lower() == "null" or str(element[key]).replace(" ","") == ""):
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
        return element

  
table_schema_dimEstadoPago = {
    'fields': [{
        'name': 'EstadoPagoID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'EstadoPagoDescripcion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoPago', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimEstadoPago_malos = {
    'fields': [{
        'name': 'EstadoPagoID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'EstadoPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoPagoDescripcion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
validador = ValidadorEstadoPagoLogic(config)
options1 = PipelineOptions(
argv= None,
runner=config['configService']['runner'],
project=config['configService']['project'],
job_name='dimestadopago-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimEstadoPago')


table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimEstadoPago_dirty')

with beam.Pipeline(options=options1) as p:     
    dimEstadoPago  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                    SELECT GENERATE_UUID() as EstadoPagoID, 'PAG' as EstadoPago, 'pagado' as  EstadoPagoDescripcion
                    UNION ALL
                    SELECT GENERATE_UUID() as EstadoPagoID, 'GEN' as EstadoPago, 'generado' as  EstadoPagoDescripcion
                    UNION ALL
                    SELECT GENERATE_UUID() as EstadoPagoID, 'PPG' as EstadoPago, 'pendiente de pago' as  EstadoPagoDescripcion
                    UNION ALL
                    SELECT GENERATE_UUID() as EstadoPagoID, 'RET' as EstadoPago, 'retenido' as  EstadoPagoDescripcion
                    UNION ALL
                    SELECT GENERATE_UUID() as EstadoPagoID, 'EDP' as EstadoPago, 'estudio derecho de pension' as  EstadoPagoDescripcion
                    UNION ALL
                    SELECT GENERATE_UUID() as EstadoPagoID, 'CAN' as EstadoPago, 'cancelado o anulado' as  EstadoPagoDescripcion
                ''',\
            use_standard_sql=True))






    dimEstadoPago_Dict = dimEstadoPago | beam.Map(lambda x:   {'EstadoPagoID':str(x['EstadoPagoID']),\
                                                               'EstadoPago':str(x['EstadoPago']),\
                                                               'EstadoPagoDescripcion':str(x['EstadoPagoDescripcion']),\
                                                                   'validacionDetected':""})



    EstadoPagoID_fullness_validated = dimEstadoPago_Dict | 'completitud EstadoPagoID' >> beam.Map(validador.fn_check_completitud,    'EstadoPagoID' )

    EstadoPago_fullness_validated = EstadoPagoID_fullness_validated | 'EstadoPago ConsecutivoDeCupon' >> beam.Map(validador.fn_check_completitud,    'EstadoPago' )





    results = EstadoPago_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x:   {'EstadoPagoID':str(x['EstadoPagoID']),\
                                                        'EstadoPago':str(x['EstadoPago']),\
                                                        'EstadoPagoDescripcion':str(x['EstadoPagoDescripcion'])})








    results["validationsDetected"] | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimEstadoPago_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimEstadoPago_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

