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
    job_name='dimtipopension-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimTipoPension')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimTipoPension_dirty')


  
table_schema_dimTipoPension = {
    'fields': [{
        'name': 'TipoPensionID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'TipoPension', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimTipoPension_malos = {
    'fields': [{
        'name': 'TipoPensionID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'TipoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'},
        ]
}



def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

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
    dimTipoPension  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                SELECT GENERATE_UUID() as TipoPensionId, TipoPension
                FROM (SELECT DISTINCT cast(TIPO_PENSION as string) as TipoPension FROM `afiliados-pensionados-prote.afiliados_pensionados.FuturaPensionados`)
                WHERE TipoPension NOT IN
                (select TipoPension from `afiliados-pensionados-prote.Datamart.dimTipoPension`)
                ''',\
            use_standard_sql=True))





    dimTipoPension_Dict = dimTipoPension | beam.Map(lambda x: \
                                                            {'TipoPensionID':str(x['TipoPensionID']),\
                                                             'TipoPension':str(x['TipoPension']),\
                                                             'validacionDetected':""})



    results = dimTipoPension_Dict | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x:  {'TipoPensionID':str(x['TipoPensionID']).encode(encoding = 'utf-8'),\
                                                                      'TipoPension':str(x['TipoPension']).encode(encoding = 'utf-8')})




    results["validationsDetected"]  | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimTipoPension_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimTipoPension,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    result = p.run()
    result.wait_until_finish()
