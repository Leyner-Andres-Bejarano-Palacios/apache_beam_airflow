# -*- coding: utf-8 -*-

import uuid
import time
import types 
import threading
import numpy as np
import pandas as pd
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.runners.runner import PipelineState
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions



options1 = PipelineOptions(
    argv= None,
    runner='DataflowRunner',
    project='afiliados-pensionados-prote',
    job_name='dimtiempoapachebeamjobname',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimTiempo')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimTiempo_dirty')


  
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



def fn_check_completitud(element,key):
    if (element[key] is None  or element[key] == "None" or element[key] == "null"):
        element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
    return element



if __name__ == "__main__":
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
