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





  
table_schema_dimEstadoBono = {
    'fields': [{
        'name': 'EstadoBonoID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'EstadoBono', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimEstadoBono_malos = {
    'fields': [{
        'name': 'EstadoBonoID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'EstadoBono', 'type':'STRING', 'mode':'NULLABLE'}
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
    job_name='dimestadobono-apache-beam-job-name',
    temp_location=config['configService']['temp_location'],
    region=config['configService']['region'],
    service_account_email=config['configService']['service_account_email'],
    save_main_session= config['configService']['save_main_session'])


    table_spec_clean = bigquery.TableReference(
        projectId=config['configService']['project'],
        datasetId='afiliados_pensionados',
        tableId='dimEstadoBono_clean')


    table_spec_dirty = bigquery.TableReference(
        projectId=config['configService']['project'],
        datasetId='afiliados_pensionados',
        tableId='dimEstadoBono_dirty')  
    dimEstadoBonoID  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                    SELECT 'None' CalculosActuarialesID, 'None' TiempoID, 'None' Inf_personasID, 'None' fechaDato, 'None' InteresTecnico,
                    'None' InflacionLargoPlazo, 'None' FactorDeslizamiento, 'None' AjusteBeneficiarios,'None' as Comision, 'None' as SupuestoDeBeneficiarios 

                ''',\
            use_standard_sql=True))





    dimEstadoBonoID_Dict = dimEstadoBonoID | beam.Map(lambda x: {'EstadoBonoID':str(x['EstadoBonoID']),\
                                                                 'EstadoBono':str(x['EstadoBono']),\
                                                                 'validacionDetected':""})



    EstadoBonoID_fullness_validated = dimEstadoBonoID_Dict | 'completitud EstadoBonoID' >> beam.Map(fn_check_completitud,    'EstadoBonoID' )

    EstadoBono_fullness_validated = EstadoBonoID_fullness_validated | 'completitud EstadoBono' >> beam.Map(fn_check_completitud,    'EstadoBono' )




    



    results = EstadoBono_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x: {'EstadoBonoID':str(x['EstadoBonoID']),\
                                                     'EstadoBono':str(x['EstadoBono'])})



    results["validacionDetected"] | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimEstadoBono_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimEstadoBono,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            
    result = p.run()
    result.wait_until_finish()

