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




table_schema_dimGestionPension = {
    'fields': [
        {
        'name':'Regional', 'type':'STRING', 'mode':'NULLABLE'},                      
        {
        'name':'Oficina', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CiudadReclamacion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InstanciaSolicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InstanciaProceso', 'type':'STRING', 'mode':'NULLABLE'},             
        {
        'name':'DepartamentoReclamacion', 'type':'STRING', 'mode':'NULLABLE'},                
        ]
}

table_schema_dimGestionPension_malos = {
    'fields': [
        {
        'name':'Regional', 'type':'STRING', 'mode':'NULLABLE'},                     
        {
        'name':'Oficina', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CiudadReclamacion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DepartamentoReclamacion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InstanciaSolicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InstanciaProceso', 'type':'STRING', 'mode':'NULLABLE'},                               
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
    job_name='dimgestionpension-apache-beam-job-name',
    temp_location=config['configService']['temp_location'],
    region=config['configService']['region'],
    service_account_email=config['configService']['service_account_email'],
    save_main_session= config['configService']['save_main_session'])


    table_spec_clean = bigquery.TableReference(
        projectId=config['configService']['project'],
        datasetId='Datamart',
        tableId='dimgestionpension')


    table_spec_dirty = bigquery.TableReference(
        projectId=config['configService']['project'],
        datasetId='Datamart',
        tableId='dimgestionpension_dirty')

    p = beam.Pipeline(options=options1)
    dimGestionPension  = (
        p
        | 'Query Table GestionPension' >> beam.io.ReadFromBigQuery(
            query='''
--------------------------------------------------------------------------------------------------
                ''',\
            use_standard_sql=True))

    dimGestionPension_Dict = dimPensionados | beam.Map(lambda x: \
                                                        {'Regional':str(x['Regional']),\
                                                         'Oficina':str(x['Oficina']),\
                                                         'InstanciaProceso':str(x['InstanciaProceso']),\
                                                         'InstanciaSolicitud':str(x['InstanciaSolicitud']),\
                                                         'CiudadReclamacion':str(x['CiudadReclamacion']),\
                                                         'DepartamentoReclamacion':str(x['DepartamentoReclamacion']),\
                                                         'validacionDetected':""})

    InstanciaSolicitud_fullness_validated = dimGestionPension_Dict | 'completitud GestionPension' >> beam.Map(fn_check_completitud,   'GestionPension')

    InstanciaSolicitud_text_validated = InstanciaSolicitud_fullness_validated | 'text GestionPension' >> beam.Map(fn_check_text,   'GestionPension')

    InstanciaProceso_fullness_validated = InstanciaSolicitud_text_validated | 'completitud InstanciaProceso' >> beam.Map(fn_check_completitud,   'InstanciaProceso')

    


    results = dimPensionados_Dict | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x:  {'Regional':str(x['Regional']),\
                                                      'CiudadReclamacion':str(x['CiudadReclamacion']),\
                                                      'InstanciaProceso':str(x['InstanciaProceso']),\
                                                      'InstanciaSolicitud':str(x['InstanciaSolicitud']),\
                                                      'DepartamentoReclamacion':str(x['DepartamentoReclamacion']),\
                                                      'Oficina':str(x['Oficina'])})


    dirty_ones = results["validationsDetected"] | | beam.Map(lambda x:  {'Regional':str(x['Regional']),\
                                                                         'Oficina':str(x['Oficina'])\
                                                                         'InstanciaProceso':str(x['InstanciaProceso']),\
                                                                         'CiudadReclamacion':str(x['CiudadReclamacion']),\
                                                                         'InstanciaSolicitud':str(x['InstanciaSolicitud']),\
                                                                         'DepartamentoReclamacion':str(x['DepartamentoReclamacion']),\
                                                                         'validacionDetected':str(x['validacionDetected'])})


    dirty_ones | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimGestionPension_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimGestionPension,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    result = p.run()
    result.wait_until_finish()                                                                        