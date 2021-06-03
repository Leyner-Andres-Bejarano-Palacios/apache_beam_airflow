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
from tableCreator import TableCreator
from google.cloud import bigquery as bq
from validaciones.validador import Validador
from apache_beam.runners.runner import PipelineState
from apache_beam.io.gcp.internal.clients import bigquery
from validaciones.helperfunctions import fn_divide_clean_dirty
from apache_beam.options.pipeline_options import PipelineOptions




table_schema_factCalificaciones = {
    'fields': [
        {
        'name':'FechaCalificacionAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaSiniestroAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EconomicaAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ComunicacionAfp', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'OrigenAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'VidaDomesticaAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoAfp1', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoAfp2', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoAfp3', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoAfp4', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoAfp5', 'type':'STRING', 'mode':'NULLABLE'},   
        {
        'name':'FechaNotificacionDictamenAfp', 'type':'STRING', 'mode':'NULLABLE'},       
        {
        'name':'CuidadoPersonalAfp', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'MovilidadAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'RolLaboralAfp', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'DiscapacidadAfp', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'MovilidadJR', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'AprendizajeAfp', 'type':'STRING', 'mode':'NULLABLE'},         
        {
        'name':'DeficienciaAfp', 'type':'STRING', 'mode':'NULLABLE'},     
        {
        'name':'FechacalificacionJN', 'type':'STRING', 'mode':'NULLABLE'},  
        {
        'name':'PrimeroJRoJN', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'DxmayorgradoJR1', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxmayorgradoJR2', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxmayorgradoJR3', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxmayorgradoJR4', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxmayorgradoJR5', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'EdadJR', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'NombreJuntaRegional', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'ComunicacionJR', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'FechaCalificacionJR', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'DiscapacidadJN', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CuidadoPersonalJR', 'type':'STRING', 'mode':'NULLABLE'},                
        {
        'name':'DxMayorGradoJN1', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoJN2', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoJN3', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoJN4', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoJN5', 'type':'STRING', 'mode':'NULLABLE'},                                        
        {
        'name':'VidaDomesticaJR', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'DeficienciaJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'EconomicaJR', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'DeficienciaJR', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'VidaDomesticaJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'AprendizajeJR', 'type':'STRING', 'mode':'NULLABLE'},       
        {
        'name':'PclJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'RolLaboralJR', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'MinusvaliaJN', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EconomicaJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'RolLaboralJN', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'ComunicacionJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'OrigenJR', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'EdadJN', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'AprendizajeJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'FechaSiniestroJN', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CuidadoPersonalJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'MinusvaliaJR', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'DiscapacidadJR', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'MovilidadJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'OrigenJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'PclJR', 'type':'STRING', 'mode':'NULLABLE'},                
        {
        'name':'FechaSiniestroJR', 'type':'STRING', 'mode':'NULLABLE'},      
        {
        'name':'MinusvaliaAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'PclAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EdadAfp', 'type':'STRING', 'mode':'NULLABLE'},                                     
        ]
}

table_schema_factCalificaciones_malos = {
    'fields': [
        {
        'name':'FechaCalificacionAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaSiniestroAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ComunicacionAfp', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'EconomicaAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CuidadoPersonalAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EdadJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'VidaDomesticaAfp', 'type':'STRING', 'mode':'NULLABLE'},                
        {
        'name':'MovilidadAfp', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'VidaDomesticaJN', 'type':'STRING', 'mode':'NULLABLE'},                       
        {
        'name':'OrigenAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DeficienciaJR', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'EconomicaJN', 'type':'STRING', 'mode':'NULLABLE'},                
        {
        'name':'MinusvaliaJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'AprendizajeJR', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'MovilidadJN', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CuidadoPersonalJN', 'type':'STRING', 'mode':'NULLABLE'},                
        {
        'name':'RolLaboralJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'MovilidadJR', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'VidaDomesticaJR', 'type':'STRING', 'mode':'NULLABLE'},                                    
        {
        'name':'DxMayorGradoAfp1', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoAfp2', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoAfp3', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoAfp4', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoAfp5', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaNotificacionDictamenAfp', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'CuidadoPersonalJR', 'type':'STRING', 'mode':'NULLABLE'},                
        {
        'name':'EdadJR', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ComunicacionJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'FechacalificacionJN', 'type':'STRING', 'mode':'NULLABLE'},             
        {
        'name':'ComunicacionJR', 'type':'STRING', 'mode':'NULLABLE'},                    
        {
        'name':'DiscapacidadJR', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'DeficienciaJN', 'type':'STRING', 'mode':'NULLABLE'},                
        {
        'name':'DxmayorgradoJR1', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxmayorgradoJR2', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxmayorgradoJR3', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxmayorgradoJR4', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxmayorgradoJR5', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'EconomicaJR', 'type':'STRING', 'mode':'NULLABLE'},                                    
        {
        'name':'RolLaboralAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'AprendizajeAfp', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'DxMayorGradoJN1', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoJN2', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoJN3', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoJN4', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DxMayorGradoJN5', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'PclJN', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'AprendizajeJN', 'type':'STRING', 'mode':'NULLABLE'},                            
        {
        'name':'DiscapacidadAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'NombreJuntaRegional', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaCalificacionJR', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'DiscapacidadJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'RolLaboralJR', 'type':'STRING', 'mode':'NULLABLE'},         
        {
        'name':'FechaSiniestroJR', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'OrigenJR', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'MinusvaliaJR', 'type':'STRING', 'mode':'NULLABLE'},                                                                
        {
        'name':'DeficienciaAfp', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'PrimeroJRoJN', 'type':'STRING', 'mode':'NULLABLE'},  
        {
        'name':'FechaSiniestroJN', 'type':'STRING', 'mode':'NULLABLE'},                             
        {
        'name':'PclAfp', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'OrigenJN', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'PclJR', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'EdadAfp', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'MinusvaliaAfp', 'type':'STRING', 'mode':'NULLABLE'},
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
    job_name='factcalificaciones-apache-beam-job-name',
    temp_location=config['configService']['temp_location'],
    region=config['configService']['region'],
    service_account_email=config['configService']['service_account_email'],
    save_main_session= config['configService']['save_main_session'])


    table_spec_clean = bigquery.TableReference(
        projectId=config['configService']['project'],
        datasetId='Datamart',
        tableId='factcalificaciones')


    table_spec_dirty = bigquery.TableReference(
        projectId=config['configService']['project'],
        datasetId='Datamart',
        tableId='factcalificaciones_dirty')


    p = beam.Pipeline(options=options1)
    factCalificaciones  = (
        p
        | 'Query Table factCalificaciones' >> beam.io.ReadFromBigQuery(
            query='''
--------------------------------------------------------------------------------------------------
                ''',\
            use_standard_sql=True))

    dimfactCalificaciones_Dict = dimPensionados | beam.Map(lambda x: \
                                                        {'FechaCalificacionAfp':str(x['FechaCalificacionAfp']),\
                                                         'FechaSiniestroAfp':str(x['FechaSiniestroAfp']),\
                                                         'EconomicaAfp':str(x['EconomicaAfp']),\
                                                         'RolLaboralAfp':str(x['RolLaboralAfp']),\
                                                         'ComunicacionAfp':str(x['ComunicacionAfp']),\
                                                         'VidaDomesticaAfp':str(x['VidaDomesticaAfp']),\
                                                         'FechaSolicitud':str(x['FechaSolicitud']),\
                                                         'DeficienciaAfp':str(x['DeficienciaAfp']),\
                                                         'EconomicaJN':str(x['EconomicaJN']),\
                                                         'DiscapacidadAfp':str(x['DiscapacidadAfp']),\
                                                         'MinusvaliaAfp':str(x['MinusvaliaAfp']),\
                                                         'AprendizajeAfp':str(x['AprendizajeAfp']),\
                                                         'CuidadoPersonalAfp':str(x['CuidadoPersonalAfp']),\
                                                         'FechaNotificacionDictamenAfp':str(x['FechaNotificacionDictamenAfp']),\
                                                         'MovilidadAfp':str(x['MovilidadAfp']),\
                                                         'DxMayorGradoAfp1':str(x['DxMayorGradoAfp1']),\
                                                         'DxMayorGradoAfp2':str(x['DxMayorGradoAfp2']),\
                                                         'DxMayorGradoAfp3':str(x['DxMayorGradoAfp3']),\
                                                         'DxMayorGradoAfp4':str(x['DxMayorGradoAfp4']),\
                                                         'DxMayorGradoAfp5':str(x['DxMayorGradoAfp5']),\
                                                         'NombreJuntaRegional':str(x['NombreJuntaRegional']),\
                                                         'FechaCalificacionJR':str(x['FechaCalificacionJR']),\                                                         
                                                         'FechaSiniestroJN':str(x['FechaSiniestroJN']),\
                                                         'VidaDomesticaJR':str(x['VidaDomesticaJR']),\
                                                         'AprendizajeJR':str(x['AprendizajeJR']),\
                                                         'DiscapacidadJR':str(x['DiscapacidadJR']),\ 
                                                         'DxmayorgradoJR1':str(x['DxmayorgradoJR1']),\
                                                         'DxmayorgradoJR2':str(x['DxmayorgradoJR2']),\
                                                         'DxmayorgradoJR3':str(x['DxmayorgradoJR3']),\
                                                         'DxmayorgradoJR4':str(x['DxmayorgradoJR4']),\
                                                         'DxmayorgradoJR5':str(x['DxmayorgradoJR5']),\
                                                         'MinusvaliaJR':str(x['MinusvaliaJR']),\
                                                         'VidaDomesticaJN':str(x['VidaDomesticaJN']),\
                                                         'AprendizajeJN':str(x['AprendizajeJN']),\
                                                         'DeficienciaJN':str(x['DeficienciaJN']),\
                                                         'DxMayorGradoJN1':str(x['DxMayorGradoJN1']),\
                                                         'DxMayorGradoJN2':str(x['DxMayorGradoJN2']),\
                                                         'DxMayorGradoJN3':str(x['DxMayorGradoJN3']),\
                                                         'DxMayorGradoJN4':str(x['DxMayorGradoJN4']),\
                                                         'DxMayorGradoJN5':str(x['DxMayorGradoJN5']),\
                                                         'FechaCalificacionJN':str(x['FechaCalificacionJN']),\
                                                         'ComunicacionJN':str(x['ComunicacionJN']),\
                                                         'MinusvaliaJN':str(x['MinusvaliaJN']),\
                                                         'CuidadoPersonalJN':str(x['CuidadoPersonalJN']),\
                                                         'MovilidadJN':str(x['MovilidadJN']),\
                                                         'RolLaboralJN':str(x['RolLaboralJN']),\
                                                         'DiscapacidadJN':str(x['DiscapacidadJN']),\
                                                         'ComunicacionJR':str(x['ComunicacionJR']),\
                                                         'PrimeroJRoJN':str(x['PrimeroJRoJN']),\
                                                         'DeficienciaJR':str(x['DeficienciaJR']),\
                                                         'PclJN':str(x['PclJN']),\
                                                         'CuidadoPersonalJR':str(x['CuidadoPersonalJR']),\
                                                         'RolLaboralJR':str(x['RolLaboralJR']),\
                                                         'OrigenJN':str(x['OrigenJN']),\
                                                         'EconomicaJR':str(x['EconomicaJR']),\
                                                         'MovilidadJR':str(x['MovilidadJR']),\
                                                         'EdadJN':str(x['EdadJN']),\                                                         
                                                         'OrigenJR':str(x['OrigenJR']),\
                                                         'EdadJR':str(x['EdadJR']),\
                                                         'OrigenAfp':str(x['OrigenAfp']),\
                                                         'PclAfp':str(x['PclAfp']),\
                                                         'PclJR':str(x['PclJR']),\
                                                         'EdadAfp':str(x['EdadAfp']),\
                                                         'validacionDetected':""})


    FechaCalificacionAfp_fullnes_validated = dimfactCalificaciones_Dict | 'completitud FechaCalificacionAfp' >> beam.Map(fn_check_completitud,   'FechaCalificacionAfp')

    FechaCalificacionAfp_dateFormat_validated = FechaCalificacionAfp_fullnes_validated | 'date format FechaCalificacionAfp' >> beam.Map(fn_check_date_format,    'FechaCalificacionAfp', "%Y%m%d" )

    FechaCalificacionAfp_dateCompare_validated = FechaCalificacionAfp_dateFormat_validated  | 'date compare FechaCalificacionAfp' >> beam.Map(fn_check_date_compare, 'FechaSolicitud', "%Y%m%d", 'FechaCalificacionAfp', "%Y%m%d")

    FechaSiniestroAfp_fullnes_validated = FechaCalificacionAfp_dateCompare_validated | 'completitud FechaSiniestroAfp' >> beam.Map(fn_check_completitud,   'FechaSiniestroAfp')

    FechaSiniestroAfp_dateFormat_validated = FechaSiniestroAfp_fullnes_validated | 'date format FechaSiniestroAfp' >> beam.Map(fn_check_date_format,    'FechaSiniestroAfp', "%Y%m%d" )

    FechaSiniestroAfp_dateCompare_validated = FechaSiniestroAfp_dateFormat_validated  | 'date compare FechaSiniestroAfp' >> beam.Map(fn_check_date_compare, 'FechaSiniestroAfp', "%Y%m%d", 'FechaCalificacionAfp', "%Y%m%d")

    OrigenAfp_fullnes_validated = FechaSiniestroAfp_dateCompare_validated | 'completitud OrigenAfp' >> beam.Map(fn_check_completitud,   'OrigenAfp')

    PclAfp_fullnes_validated = OrigenAfp_fullnes_validated | 'completitud PclAfp' >> beam.Map(fn_check_completitud,   'PclAfp')

    FechaNotificacionDictamenAfp_dateCompare_validated = PclAfp_fullnes_validated  | 'date compare FechaNotificacionDictamenAfp' >> beam.Map(fn_check_date_compare, 'FechaCalificacionAfp', "%Y%m%d", 'FechaNotificacionDictamenAfp', "%Y%m%d")

    FechaCalificacionJR_dateCompare_validated = FechaNotificacionDictamenAfp_dateCompare_validated   | 'date compare FechaCalificacionJR' >> beam.Map(fn_check_date_compare, 'FechaSolicitud', "%Y%m%d", 'FechaCalificacionJR', "%Y%m%d")

    FechaCalificacionJR_dateCompare_validated2 = FechaCalificacionJR_dateCompare_validated   | 'date compare FechaCalificacionJR 2' >> beam.Map(fn_check_date_compare, 'FechaCalificacionAfp', "%Y%m%d", 'FechaCalificacionJR', "%Y%m%d")

    FechaCalificacionJR_fullnes_validated = FechaCalificacionJR_dateCompare_validated2 | 'completitud FechaCalificacionJR' >> beam.Map(fn_check_completitud,   'FechaCalificacionJR')

    FechaCalificacionJR_dateFormat_validated = FechaCalificacionJR_fullnes_validated | 'date format FechaCalificacionJR' >> beam.Map(fn_check_date_format,    'FechaCalificacionJR', "%Y%m%d" )

    FechaSiniestroJR_fullnes_validated = FechaCalificacionJR_dateFormat_validated | 'completitud FechaSiniestroJR' >> beam.Map(fn_check_completitud,   'FechaSiniestroJR')

    FechaSiniestroJR_dateFormat_validated = FechaSiniestroJR_fullnes_validated | 'date format FechaSiniestroJR' >> beam.Map(fn_check_date_format,    'FechaSiniestroJR', "%Y%m%d" )

    FechaSiniestroJR_dateCompare_validated = FechaSiniestroJR_dateFormat_validated   | 'date compare FechaSiniestroAfp' >> beam.Map(fn_check_date_compare, 'FechaSiniestroJR', "%Y%m%d", 'FechaCalificacionJR', "%Y%m%d")

    OrigenJR_fullnes_validated = FechaSiniestroJR_dateCompare_validated | 'completitud OrigenJR' >> beam.Map(fn_check_completitud,   'OrigenJR')

    FechaCalificacionJN_fullnes_validated = OrigenJR_fullnes_validated | 'completitud FechaCalificacionJN' >> beam.Map(fn_check_completitud,   'FechaCalificacionJN')

    FechaCalificacionJN_dateFormat_validated = FechaCalificacionJN_fullnes_validated | 'date format FechaCalificacionJN' >> beam.Map(fn_check_date_format,    'FechaCalificacionJN', "%Y%m%d" )

    FechaCalificacionJN_dateCompare_validated = FechaCalificacionJN_dateFormat_validated  | 'date compare FechaCalificacionJN' >> beam.Map(fn_check_date_compare, 'FechaSolicitud', "%Y%m%d","FechaCalificacionJN", "%Y%m%d")

    FechaCalificacionJN_dateCompare_validated2 = FechaCalificacionJN_dateCompare_validated  | 'date compare FechaCalificacionJN 2' >> beam.Map(fn_check_date_compare, 'FechaCalificacionAfp', "%Y%m%d","FechaCalificacionJN", "%Y%m%d")

    FechaCalificacionJN_dateCompare_validated3 = FechaCalificacionJN_dateCompare_validated2  | 'date compare FechaCalificacionJN 3' >> beam.Map(fn_check_date_compare, 'FechaCalificacionJR', "%Y%m%d","FechaCalificacionJN", "%Y%m%d")

    FechaSiniestroJN_fullnes_validated = FechaCalificacionJN_dateCompare_validated3 | 'completitud FechaSiniestroJN' >> beam.Map(fn_check_completitud,   'FechaSiniestroJN')

    FechaSiniestroJN_dateFormat_validated = FechaSiniestroJN_fullnes_validated | 'date format FechaSiniestroJN' >> beam.Map(fn_check_date_format,    'FechaSiniestroJN', "%Y%m%d" )

    FechaSiniestroJN_dateCompare_validated = FechaSiniestroJN_dateFormat_validated   | 'date compare FechaSiniestroJN' >> beam.Map(fn_check_date_compare, 'FechaSiniestroJN', "%Y%m%d", 'FechaCalificacionJN', "%Y%m%d")

    OrigenJN_fullnes_validated = FechaSiniestroJN_dateCompare_validated | 'completitud OrigenJN' >> beam.Map(fn_check_completitud,   'OrigenJN')

    PclJN_fullnes_validated = OrigenJN_fullnes_validated | 'completitud PclJN' >> beam.Map(fn_check_completitud,   'PclJN')



    results = FechaSiniestroJR_dateCompare_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x:  {'FechaCalificacionAfp':str(x['FechaCalificacionAfp']),\
                                                      'RolLaboralAfp':str(x['RolLaboralAfp']),\
                                                      'EconomicaAfp':str(x['EconomicaAfp']),\
                                                      'VidaDomesticaAfp':str(x['VidaDomesticaAfp']),\
                                                      'ComunicacionAfp':str(x['ComunicacionAfp']),\
                                                      'DeficienciaAfp':str(x['DeficienciaAfp']),\
                                                      'DiscapacidadAfp':str(x['DiscapacidadAfp']),\
                                                      'RolLaboralJN':str(x['RolLaboralJN']),\
                                                      'CuidadoPersonalAfp':str(x['CuidadoPersonalAfp']),\
                                                      'MinusvaliaAfp':str(x['MinusvaliaAfp']),\
                                                      'AprendizajeAfp':str(x['AprendizajeAfp']),\
                                                      'MovilidadAfp':str(x['MovilidadAfp']),\
                                                      'DxMayorGradoAfp1':str(x['DxMayorGradoAfp1']),\
                                                      'DxMayorGradoAfp2':str(x['DxMayorGradoAfp2']),\
                                                      'DxMayorGradoAfp3':str(x['DxMayorGradoAfp3']),\
                                                      'DxMayorGradoAfp4':str(x['DxMayorGradoAfp4']),\
                                                      'DxMayorGradoAfp5':str(x['DxMayorGradoAfp5']),\ 
                                                      'FechaNotificacionDictamenAfp':str(x['FechaNotificacionDictamenAfp']),\ 
                                                      'NombreJuntaRegional':str(x['NombreJuntaRegional']),\
                                                      'FechaCalificacionJR':str(x['FechaCalificacionJR']),\
                                                      'FechaSiniestroJR':str(x['FechaSiniestroJR']),\
                                                      'CuidadoPersonalJR':str(x['CuidadoPersonalJR']),\
                                                      'EconomicaJN':str(x['EconomicaJN']),\
                                                      'VidaDomesticaJN':str(x['VidaDomesticaJN']),\
                                                      'VidaDomesticaJR':str(x['VidaDomesticaJR']),\
                                                      'DxmayorgradoJR1':str(x['DxmayorgradoJR1']),\
                                                      'DxmayorgradoJR2':str(x['DxmayorgradoJR2']),\
                                                      'DxmayorgradoJR3':str(x['DxmayorgradoJR3']),\
                                                      'DxmayorgradoJR4':str(x['DxmayorgradoJR4']),\
                                                      'DxmayorgradoJR5':str(x['DxmayorgradoJR5']),\ 
                                                      'DeficienciaJN':str(x['DeficienciaJN']),\
                                                      'MovilidadJN':str(x['MovilidadJN']),\
                                                      'EdadJN':str(x['EdadJN']),\
                                                      'ComunicacionJN':str(x['ComunicacionJN']),\
                                                      'CuidadoPersonalJN':str(x['CuidadoPersonalJN']),\
                                                      'DiscapacidadJN':str(x['DiscapacidadJN']),\
                                                      'FechaSiniestroJN':str(x['FechaSiniestroJN']),\                                                     
                                                      'MovilidadJR':str(x['MovilidadJR']),\
                                                      'MinusvaliaJN':str(x['MinusvaliaJN']),\
                                                      'AprendizajeJN':str(x['AprendizajeJN']),\
                                                      'PclJN':str(x['PclJN']),\
                                                      'DxMayorGradoJN1':str(x['DxMayorGradoJN1']),\
                                                      'DxMayorGradoJN2':str(x['DxMayorGradoJN2']),\
                                                      'DxMayorGradoJN3':str(x['DxMayorGradoJN3']),\
                                                      'DxMayorGradoJN4':str(x['DxMayorGradoJN4']),\
                                                      'DxMayorGradoJN5':str(x['DxMayorGradoJN5']),\                                                      
                                                      'DiscapacidadJR':str(x['DiscapacidadJR']),\
                                                      'ComunicacionJR':str(x['ComunicacionJR']),\
                                                      'FechaCalificacionJN':str(x['FechaCalificacionJN']),\
                                                      'MinusvaliaJR':str(x['MinusvaliaJR']),\
                                                      'PrimeroJRoJN':str(x['PrimeroJRoJN']),\ 
                                                      'DeficienciaJR':str(x['DeficienciaJR']),\ 
                                                      'RolLaboralJR':str(x['RolLaboralJR']),\  
                                                      'EconomicaJR':str(x['EconomicaJR']),\
                                                      'OrigenJN':str(x['OrigenJN']),\
                                                      'AprendizajeJR':str(x['AprendizajeJR']),\                   
                                                      'OrigenAfp':str(x['OrigenAfp']),\
                                                      'EdadJR':str(x['EdadJR']),\
                                                      'OrigenJR':str(x['OrigenJR']),\
                                                      'PclAfp':str(x['PclAfp']),\
                                                      'PclJR':str(x['PclJR']),\
                                                      'EdadAfp':str(x['EdadAfp']),\
                                                      'FechaSiniestroAfp':str(x['FechaSiniestroAfp'])})


    dirty_ones = results["validationsDetected"] | | beam.Map(lambda x:  {'FechaCalificacionAfp':str(x['FechaCalificacionAfp']),\
                                                                         'RolLaboralAfp':str(x['RolLaboralAfp']),\
                                                                         'EconomicaAfp':str(x['EconomicaAfp']),\
                                                                         'DeficienciaAfp':str(x['DeficienciaAfp']),\
                                                                         'DiscapacidadAfp':str(x['DiscapacidadAfp']),\
                                                                         'FechaSiniestroAfp':str(x['FechaSiniestroAfp']),\
                                                                         'MinusvaliaAfp':str(x['MinusvaliaAfp']),\
                                                                         'RolLaboralJN':str(x['RolLaboralJN']),\
                                                                         'VidaDomesticaAfp':str(x['VidaDomesticaAfp']),\
                                                                         'AprendizajeAfp':str(x['AprendizajeAfp']),\
                                                                         'ComunicacionAfp':str(x['ComunicacionAfp']),\
                                                                         'CuidadoPersonalAfp':str(x['CuidadoPersonalAfp']),\
                                                                         'DxMayorGradoAfp1':str(x['DxMayorGradoAfp1']),\
                                                                         'DxMayorGradoAfp2':str(x['DxMayorGradoAfp2']),\
                                                                         'DxMayorGradoAfp3':str(x['DxMayorGradoAfp3']),\
                                                                         'DxMayorGradoAfp4':str(x['DxMayorGradoAfp4']),\
                                                                         'DxMayorGradoAfp5':str(x['DxMayorGradoAfp5']),\ 
                                                                         'FechaNotificacionDictamenAfp':str(x['FechaNotificacionDictamenAfp']),\ 
                                                                         'NombreJuntaRegional':str(x['NombreJuntaRegional']),\
                                                                         'FechaCalificacionJR':str(x['FechaCalificacionJR']),\
                                                                         'FechaSiniestroJR':str(x['FechaSiniestroJR']),\
                                                                         'FechaCalificacionJN':str(x['FechaCalificacionJN']),\
                                                                         'DiscapacidadJR':str(x['DiscapacidadJR']),\
                                                                         'VidaDomesticaJN':str(x['VidaDomesticaJN']),\
                                                                         'CuidadoPersonalJN':str(x['CuidadoPersonalJN']),\
                                                                         'ComunicacionJR':str(x['ComunicacionJR']),\
                                                                         'MinusvaliaJN':str(x['MinusvaliaJN']),\
                                                                         'CuidadoPersonalJR':str(x['CuidadoPersonalJR']),\
                                                                         'VidaDomesticaJR':str(x['VidaDomesticaJR']),\
                                                                         'DxmayorgradoJR1':str(x['DxmayorgradoJR1']),\
                                                                         'DxmayorgradoJR2':str(x['DxmayorgradoJR2']),\
                                                                         'DxmayorgradoJR3':str(x['DxmayorgradoJR3']),\
                                                                         'DxmayorgradoJR4':str(x['DxmayorgradoJR4']),\
                                                                         'DxmayorgradoJR5':str(x['DxmayorgradoJR5']),\ 
                                                                         'FechaSiniestroJN':str(x['FechaSiniestroJN']),\ 
                                                                         'OrigenJN':str(x['OrigenJN']),\   
                                                                         'PclJN':str(x['PclJN']),\  
                                                                         'EdadJN':str(x['EdadJN']),\ 
                                                                         'MovilidadJN':str(x['MovilidadJN']),\ 
                                                                         'ComunicacionJN':str(x['ComunicacionJN']),\ 
                                                                         'AprendizajeJN':str(x['AprendizajeJN']),\                                                               
                                                                         'MinusvaliaJR':str(x['MinusvaliaJR']),\
                                                                         'PrimeroJRoJN':str(x['PrimeroJRoJN']),\                                                                       
                                                                         'MovilidadAfp':str(x['MovilidadAfp']),\
                                                                         'DxMayorGradoJN1':str(x['DxMayorGradoJN1']),\
                                                                         'DxMayorGradoJN2':str(x['DxMayorGradoJN2']),\
                                                                         'DxMayorGradoJN3':str(x['DxMayorGradoJN3']),\
                                                                         'DxMayorGradoJN4':str(x['DxMayorGradoJN4']),\
                                                                         'DxMayorGradoJN5':str(x['DxMayorGradoJN5']),\                                                                         
                                                                         'DeficienciaJR':str(x['DeficienciaJR']),\
                                                                         'RolLaboralJR':str(x['RolLaboralJR']),\
                                                                         'EconomicaJR':str(x['EconomicaJR']),\
                                                                         'EconomicaJN':str(x['EconomicaJN']),\
                                                                         'AprendizajeJR':str(x['AprendizajeJR']),\
                                                                         'DeficienciaJN':str(x['DeficienciaJN']),\
                                                                         'DiscapacidadJN':str(x['DiscapacidadJN']),\
                                                                         'MovilidadJR':str(x['MovilidadJR']),\
                                                                         'EdadJR':str(x['EdadJR']),\
                                                                         'OrigenAfp':str(x['OrigenAfp']),\
                                                                         'OrigenJR':str(x['OrigenJR']),\
                                                                         'PclAfp':str(x['PclAfp']),\
                                                                         'PclJR':str(x['PclJR']),\
                                                                         'EdadAfp':str(x['EdadAfp']),\
                                                                         'validacionDetected':str(x['validacionDetected'])})


    dirty_ones | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_factCalificaciones_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_factCalificaciones,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    result = p.run()
    result.wait_until_finish()                                                                        