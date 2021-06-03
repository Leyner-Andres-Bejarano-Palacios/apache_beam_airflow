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
from tableCreator import TableCreator
from google.cloud import bigquery as bq
from validaciones.validador import Validador
from apache_beam.runners.runner import PipelineState
from apache_beam.io.gcp.internal.clients import bigquery
from validaciones.helperfunctions import fn_divide_clean_dirty
from apache_beam.options.pipeline_options import PipelineOptions




table_schema_factProcesosJuridicos = {
    'fields': [
        {
        'name':'FechaNotificacionPJ', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoProceso', 'type':'STRING', 'mode':'NULLABLE'},      
        ]
}

table_schema_factProcesosJuridicos_malos = {
    'fields': [
        {
        'name':'FechaNotificacionPJ', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoProceso', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'},        
        ]
}

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('/home/airflow/gcs/data/repo/configs/config.ini')
    validador = Validador(config)
    options1 = PipelineOptions(
    argv= None,
    runner=config['configService']['runner'],
    project=config['configService']['project'],
    job_name='factprocesosjuridicos-apache-beam-job-name',
    temp_location=config['configService']['temp_location'],
    region=config['configService']['region'],
    service_account_email=config['configService']['service_account_email'],
    save_main_session= config['configService']['save_main_session'])


    table_spec_clean = bigquery.TableReference(
        projectId=config['configService']['project'],
        datasetId='Datamart',
        tableId='factprocesosjuridicos')


    table_spec_dirty = bigquery.TableReference(
        projectId=config['configService']['project'],
        datasetId='Datamart',
        tableId='factprocesosjuridicos_dirty')

    p = beam.Pipeline(options=options1)
    factProcesosJuridicos  = (
        p
        | 'Query Table factProcesosJuridicos' >> beam.io.ReadFromBigQuery(
            query='''
--------------------------------------------------------------------------------------------------
                ''',\
            use_standard_sql=True))

    dimPensionados_Dict = dimPensionados | beam.Map(lambda x: \
                                                        {'FechaNotificacionPJ':str(x['FechaNotificacionPJ']),\
                                                        {'TipoProceso':str(x['TipoProceso']),\
                                                         'validacionDetected':""})


    results = dimPensionados_Dict | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x:  {'TipoProceso':str(x['TipoProceso']),\
                                                      'FechaNotificacionPJ':str(x['FechaNotificacionPJ'])})


    dirty_ones = results["validationsDetected"] | | beam.Map(lambda x:  {'TipoProceso':str(x['TipoProceso']),\
                                                                         'FechaNotificacionPJ':str(x['FechaNotificacionPJ']),\
                                                                         'validacionDetected':str(x['validacionDetected'])})


    dirty_ones | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_factIncapacidadesTemporales_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_factIncapacidadesTemporales,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    result = p.run()
    result.wait_until_finish()                                                                        