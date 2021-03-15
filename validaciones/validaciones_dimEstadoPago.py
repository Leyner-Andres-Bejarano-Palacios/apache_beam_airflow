# -*- coding: utf-8 -*-

import time
import uuid
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
    job_name='dimestadopago-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimEstadoPago')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimEstadoPago_dirty')


  
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

def fn_check_value_in(key,listValues):
    correct = False
    for value in listValues:
        if element[key] == value:
            correct = True
            break
    if correct == False:
        element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado en lista,"
    return element

def fn_check_numbers(element,key):
    correct = False
    if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None"):
        if (element[key].isnumeric() == True):
            pass
        else:
            correct = True
            element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no son numeros,"
    return element


def fn_check_text(element,key):
    if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None"):
        if (str(element[key]).replace(" ","").isalpha() == False):
            element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no es texto,"
    return element



if __name__ == "__main__":
    p = beam.Pipeline(options=options1)
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



    EstadoPagoID_fullness_validated = dimEstadoPago_Dict | 'completitud EstadoPagoID' >> beam.Map(fn_check_completitud,    'EstadoPagoID' )

    EstadoPago_fullness_validated = EstadoPagoID_fullness_validated | 'EstadoPago ConsecutivoDeCupon' >> beam.Map(fn_check_completitud,    'EstadoPago' )





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
    result = p.run()
    result.wait_until_finish()
