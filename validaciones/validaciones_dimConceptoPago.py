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



  
table_schema_dimConceptoPago = {
    'fields': [{
        'name': 'ConceptoPagoID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {
        'name':'ConceptoPago', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimConceptoPago_malos = {
    'fields': [{
        'name': 'ConceptoPagoID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {
        'name':'ConceptoPago', 'type':'STRING', 'mode':'NULLABLE'},
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
job_name='dimconceptopago-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])
table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimConceptoPago')
table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_dimConceptoPago_dirty')

with beam.Pipeline(options=options1) as p:       
    dimConceptoPago  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                SELECT GENERATE_UUID() as ConceptoPagoID, 'Mesada actual' as ConceptoPago
                UNION ALL
                select GENERATE_UUID() as ConceptoPagoID, 'Valor Devolucion' as ConceptoPago
                ''',\
            use_standard_sql=True))






    dimConceptoPago_Dict = dimConceptoPago | beam.Map(lambda x:   {'ConceptoPagoID':str(x['ConceptoPagoID']),\
                                                                   'ConceptoPago':str(x['ConceptoPago']),\
                                                                   'validacionDetected':""})





    results = dimConceptoPago_Dict | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x:   {'ConceptoPagoID':str(x['ConceptoPagoID']),\
                                                       'ConceptoPago':str(x['ConceptoPago'])})








    results["validationsDetected"] | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimConceptoPago_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimConceptoPago_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
