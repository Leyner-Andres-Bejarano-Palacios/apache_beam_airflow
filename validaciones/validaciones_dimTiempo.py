# -*- coding: utf-8 -*-




import time
import uuid
import types
import threading
import numpy as np
import pandas as pd
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




  
table_schema_dimTiempo = {
    'fields': [{
        'name': 'date', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'Anno', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Semana', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Trimestre', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimTiempo_malos = {
    'fields': [{
        'name': 'date', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'Anno', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Semana', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Trimestre', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        
        ]
}



if __name__ == "__main__":
    tableCreator = TableCreator()
    validador = Validador(tableCreator._vConfig)
    options1 = PipelineOptions(
    argv= None,
    runner=config['configService']['runner'],
    project=config['configService']['project'],
    job_name='dimtiempoapachebeamjobname',
    temp_location=config['configService']['temp_location'],
    region=config['configService']['region'],
    service_account_email=config['configService']['service_account_email'],
    save_main_session= config['configService']['save_main_session'])


    table_spec_clean = bigquery.TableReference(
        projectId=config['configService']['project'],
        datasetId='Datamart',
        tableId='dimTiempo')


    table_spec_dirty = bigquery.TableReference(
        projectId=config['configService']['project'],
        datasetId='Datamart',
        tableId='dimTiempo_dirty')     
        
    p = beam.Pipeline(options=options1)        
    dimTiempo  = (
        p
        | 'Query Table dimTiempo' >> beam.io.ReadFromBigQuery(
            query='''
            SELECT date,
            EXTRACT(YEAR FROM date) AS Anno,
            EXTRACT(WEEK FROM date) AS Semana,
            EXTRACT(QUARTER FROM date) AS Trimestre,
            FROM UNNEST(GENERATE_DATE_ARRAY('2021-01-01', '2021-12-31')) AS date
            ORDER BY date; 
                ''',\
            use_standard_sql=True))




    dimTiempo_Dict = dimTiempo | beam.Map(lambda x:  {'date':str(x['date']),\
                                                        'Anno':str(x['Anno']),\
                                                        'Semana':str(x['Semana']),\
                                                        'Trimestre':str(x['Trimestre']),\
                                                        'validacionDetected':""})




    dimTiempo_fullness_validated = dimTiempo_Dict | 'completitud date' >> beam.Map(fn_check_completitud,    'date' )

    results = dimTiempo_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()

    limpias = results["Clean"] | beam.Map(lambda x:  {'date':str(x['date']),\
                                                        'Anno':str(x['Anno']),\
                                                        'Semana':str(x['Semana']),\
                                                        'Trimestre':str(x['Trimestre'])})



    results["validationsDetected"] | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimTiempo_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimTiempo,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            
    result = p.run()
    result.wait_until_finish()
