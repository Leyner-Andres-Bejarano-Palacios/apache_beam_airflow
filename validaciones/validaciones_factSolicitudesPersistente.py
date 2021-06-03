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


class ValidadorfactSolicitudesLogic():
    def __init__(self, config):
        self._vName = self.__class__.__name__
        self._vConfig = config

    @staticmethod
    def fn_check_completitud(element,key):
        if (element[key] is None  or str(element[key]).lower() == "none" or (element[key]).lower() == "null" or str(element[key]).replace(" ","") == ""):
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
        return element

    @staticmethod
    def fn_check_date_compare(element,key1,dateFormat1,key2,dateFormat2):
        if (element[key1] is not None  and \
            element[key1] != "None" and \
            element[key1] != "null") and \
           (element[key2] is not None  and \
            element[key2] != "None"  and \
            element[key2] != "null"): 
            try:
                fechasiniestro  = datetime.strptime(element[key1], dateFormat1)
                fechaNacimiento = datetime.strptime(element[key2], dateFormat2)
                if fechaNacimiento >= fechasiniestro:
                    element["validacionDetected"] = element["validacionDetected"] + key2 + " mayor a " + key1 + ","      
            except:
                element["validacionDetected"] = element["validacionDetected"] + str(key1) +" o "+str(key2)+""+" tiene un formato de fecha invalida," 
            finally:
                return element
        else:
            return element         


  
table_schema_factSolicitudesPersistente = {
    'fields': [
        {
        'name':'RegistroActivo', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaInicioVigencia', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaFinVigencia', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'NumeroSolicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoSolicitud', 'type':'STRING', 'mode':'NULLABLE'},                
        {
        'name':'FechaSolicitud', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}


#falta pasar subnetwork
config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
validador = ValidadorfactSolicitudesLogic(config)
options1 = PipelineOptions(
argv= None,
runner=config['configService']['runner'],
project=config['configService']['project'],
job_name='factsolicitudes-persistente-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_factSolicitudesPersistente')


with beam.Pipeline(options=options1) as p:
    dimTipoSolicitud  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                    SELECT 'True' RegistroActivo,CURRENT_DATE()  AS FechaInicioVigencia ,
                    NULL AS FechaFinVigencia, NumeroSolicitud, EstadoSolicitud, FechaSolicitud
                    FROM
                    (SELECT EstadoSolicitud, NumeroSolicitud, FechaSolicitud
                    FROM '''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_factSolicitudes
                    
                    UNION ALL 
                    
                    SELECT EstadoSolicitud, NumeroSolicitud, FechaSolicitud
                    FROM `'''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_factSolicitudes_dirty`)
                ''',\
            use_standard_sql=True))





    factSolicitudesPersistente = dimTipoSolicitud | beam.Map(lambda x: {'RegistroActivo':str(x['RegistroActivo']),\
                                                                  'FechaInicioVigencia':str(x['FechaInicioVigencia']),\
                                                                  'FechaFinVigencia':str(x['FechaFinVigencia']),\
                                                                  'NumeroSolicitud':str(x['NumeroSolicitud']),\
                                                                  'EstadoSolicitud':str(x['EstadoSolicitud']),\
                                                                  'FechaSolicitud':str(x['FechaSolicitud'])})




    



    factSolicitudesPersistente |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_factSolicitudesPersistente,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


