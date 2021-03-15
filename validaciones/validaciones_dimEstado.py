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
    job_name='dimestado-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimEstado')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimEstado_dirty')


  
table_schema_dimEstado = {
    'fields': [{
        'name': 'EstadoID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'Estado', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Atributo', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Descripcion', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimEstado_malos = {
    'fields': [{
        'name': 'EstadoID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'Estado', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Atributo', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Descripcion', 'type':'STRING', 'mode':'NULLABLE'},
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
    dimEstado  = (
        p
        | 'Query Table dimEstado' >> beam.io.ReadFromBigQuery(
            query='''
                SELECT GENERATE_UUID() as EstadoID, Estado, Descripcion, Atributo FROM
                (SELECT distinct CODIGO_ESTADO_PENSION as Estado, DESCRIPCION_ESTADO_PENSION AS Descripcion , "Pension" Atributo
                FROM `afiliados-pensionados-prote.afiliados_pensionados.ESTADO_PENSION`
                UNION ALL
                SELECT distinct CODIGO_ESTADO as EstadoD,DESCRIPCION_ESTADO as Descripcion, "Solicitud" Atributo
                FROM `afiliados-pensionados-prote.afiliados_pensionados.ESTADO_SOLICITUD`)
                ''',\
            use_standard_sql=True))




    dimEstado_Dict = dimEstado | beam.Map(lambda x: \
                                                            {'EstadoID':str(x['EstadoID']),\
                                                             'Estado':str(x['Estado']),\
                                                             'Descripcion':str(x['Descripcion']),\
                                                             'Atributo':str(x['Atributo']),\
                                                             'validacionDetected':""})




    EstadoSolicitud_fullness_validated = dimEstado_Dict | 'completitud EstadoSolicitud' >> beam.Map(fn_check_completitud,    'Estado' )
    #. validaciones  EstadoSolicitudID
    Atributo_fullness_validated = dimEstado_Dict | 'completitud Atributo' >> beam.Map(fn_check_completitud,    'Atributo' )

    results = Atributo_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()

    limpias = results["Clean"] | beam.Map(lambda x: \
                                                            {'EstadoID':str(x['EstadoID']),\
                                                             'Estado':str(x['Estado']),\
                                                             'Descripcion':str(x['Descripcion']),\
                                                             'Atributo':str(x['Atributo'])})



    results["validationsDetected"] | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimEstado_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimEstado,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            
    result = p.run()
    result.wait_until_finish()