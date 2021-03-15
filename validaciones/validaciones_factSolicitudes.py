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
    job_name='factsolicitudes-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='factSolicitudes')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='factSolicitudes_dirty')


  
table_schema_factSolicitudes = {
    'fields': [
        {
        'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Numero_Solicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoSolicitudID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        ]
}

table_schema_factSolicitudes_malos = {
    'fields': [
        {
        'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Numero_Solicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoSolicitudID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'},
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
                SELECT  "None" TiempoID, Numero_Solicitud, e.EstadoID, b.TipoSolicitudID, a.InfPersonasID, CURRENT_DATE() as FechaDato
                FROM

                afiliados-pensionados-prote.Datamart.dimInfPersonas a

                LEFT JOIN
                (SELECT PensionadosID, DocumentoDeLaPersona FROM
                afiliados-pensionados-prote.Datamart.dimPensionados c
                UNION ALL
                SELECT BeneficiariosID, IdentificacionAfiliado FROM
                afiliados-pensionados-prote.Datamart.dimBeneficiarios d
                ) f
                ON
                f.PensionadosID=a.PensionadosId

                LEFT JOIN
                afiliados-pensionados-prote.afiliados_pensionados.PenFutura pa
                ON
                pa.idAfiliado=f.DocumentoDeLaPersona

                LEFT JOIN
                afiliados-pensionados-prote.Datamart.dimTipoSolicitud b
                ON
                b.TipoSolicitud=pa.TIPO_SOLICITUD
                LEFT JOIN
                afiliados-pensionados-prote.Datamart.dimEstado e
                on
                e.Estado=pa.ESTADO_PENSION /*falta asociar el campo EstadoSolicitud*/
                ''',\
            use_standard_sql=True))





    dimTipoSolicitud = dimTipoSolicitud | beam.Map(lambda x: {'TiempoID':str(x['TiempoID']),\
                                                              'EstadoID':str(x['EstadoID']),\
                                                              'Numero_Solicitud':str(x['Numero_Solicitud']),\
                                                              'TipoSolicitudID':str(x['TipoSolicitudID']),\
                                                              'InfPersonasID':str(x['InfPersonasID']),\
                                                              'FechaDato':str(x['FechaDato']),\
                                                              'validacionDetected':""})




    TiempoID_fullness_validated = dimTipoSolicitud | 'completitud TiempoID' >> beam.Map(fn_check_completitud,    'TiempoID' )

    EstadoID_fullness_validated = TiempoID_fullness_validated | 'completitud EstadoID' >> beam.Map(fn_check_completitud,    'EstadoID' )

    Numero_Solicitud_fullness_validated = EstadoID_fullness_validated | 'completitud Numero_Solicitud' >> beam.Map(fn_check_completitud,    'Numero_Solicitud' )

    TipoSolicitudID_fullness_validated = Numero_Solicitud_fullness_validated | 'completitud TipoSolicitudID' >> beam.Map(fn_check_completitud,    'TipoSolicitudID' )

    InfPersonasID_fullness_validated = TipoSolicitudID_fullness_validated | 'completitud InfPersonasID' >> beam.Map(fn_check_completitud,    'InfPersonasID' )

    FechaDato_fullness_validated = InfPersonasID_fullness_validated | 'completitud FechaDato' >> beam.Map(fn_check_completitud,    'FechaDato' )






    



    results = FechaDato_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x: {'TiempoID':str(x['TiempoID']),\
                                                     'EstadoID':str(x['EstadoID']),\
                                                     'Numero_Solicitud':str(x['Numero_Solicitud']),\
                                                     'TipoSolicitudID':str(x['TipoSolicitudID']),\
                                                     'InfPersonasID':str(x['InfPersonasID']),\
                                                     'FechaDato':str(x['FechaDato'])})



    results["validacionDetected"] | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_factSolicitudes_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_factSolicitudes,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    p.run()
