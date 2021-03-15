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
    job_name='dimtiposolicitud-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimTipoSolicitud')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimTipoSolicitud_dirty')


  
table_schema_dimTipoSolicitud = {
    'fields': [{
        'name': 'TipoSolicitudID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'TipoSolicitud', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimTipoSolicitud_malos = {
    'fields': [{
        'name': 'TipoSolicitudID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'TipoSolicitud', 'type':'STRING', 'mode':'NULLABLE'},
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
    dimTipoSolicitud  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                SELECT distinct TIPO_SOLICITUD as TipoSolicitud, GENERATE_UUID() as TipoSolicitudID
                FROM`afiliados-pensionados-prote.afiliados_pensionados.FuturaPensionados`
                group by
                TipoSolicitud
                ''',\
            use_standard_sql=True))





    dimTipoSolicitud = dimTipoSolicitud | beam.Map(lambda x: {'TipoSolicitudID':str(x['TipoSolicitudID']),\
                                                              'TipoSolicitud':str(x['TipoSolicitud']),\
                                                              'validacionDetected':""})



    TipoSolicitudID_fullness_validated = dimTipoSolicitud | 'completitud TipoSolicitudID' >> beam.Map(fn_check_completitud,    'TipoSolicitudID' )

    TipoSolicitud_fullness_validated = TipoSolicitudID_fullness_validated | 'completitud TipoSolicitud' >> beam.Map(fn_check_completitud,    'TipoSolicitud' )






    



    results = TipoSolicitud_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    validadas = results["validationsDetected"] | beam.Map(lambda x: {'TipoSolicitudID':str(x['TipoSolicitudID']),\
                                                              'TipoSolicitud':str(x['TipoSolicitud']),\
                                                              'validacionDetected':x['validacionDetected']})



    limpias = results["Clean"] | beam.Map(lambda x: {'TipoSolicitudID':str(x['TipoSolicitudID']),\
                                                              'TipoSolicitud':str(x['TipoSolicitud'])})



    validadas | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimTipoSolicitud_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimTipoSolicitud,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    result = p.run()
    result.wait_until_finish()
