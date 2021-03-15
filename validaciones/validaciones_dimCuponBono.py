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
    job_name='dimcuponesbono-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimCuponesBono')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimCuponesBono_dirty')


  
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
            element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no son texto,"
    return element



if __name__ == "__main__":
    p = beam.Pipeline(options=options1)
    dimInfPersonas  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
            select GENERATE_UUID() as CuponesBonoID, ConsecutivoCupon, EstadoCuponID FROM
            (SELECT distinct a.CONSECUTIVO_CUPON as ConsecutivoCupon, b.EstadoCuponID
            FROM `afiliados-pensionados-prote.afiliados_pensionados.SUPERCUPON` a
            LEFT JOIN
            `afiliados-pensionados-prote.Datamart.dimEstadoCupon` b
            ON
            a.ESTADO_CUPON=b.EstadoCupon)
                ''',\
            use_standard_sql=True))






    dimInfPersonas_Dict = dimInfPersonas | beam.Map(lambda x:   {'CuponesBonoID':str(x['CuponesBonoID']),\
                                                                 'ConsecutivoCupon':str(x['ConsecutivoCupon']),\
                                                                 'EstadoCuponID':str(x['EstadoCuponID']),\
                                                                 'validacionDetected':""})



    CuponesBonoID_fullness_validated = dimInfPersonas_Dict | 'completitud CuponesBonoID' >> beam.Map(fn_check_completitud,    'CuponesBonoID' )

    ConsecutivoCupon_fullness_validated = CuponesBonoID_fullness_validated | 'completitud ConsecutivoCupon' >> beam.Map(fn_check_completitud,    'ConsecutivoCupon' )

    EstadoCuponID_fullness_validated = ConsecutivoCupon_fullness_validated | 'completitud EstadoCuponID' >> beam.Map(fn_check_completitud,    'EstadoCuponID' )





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
    p.run()
