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





  
table_schema_FactAhorroPensional = {
    'fields': [{
        'name': 'AhorroPensionalID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'saldoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SumaAdicional', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CapitalNecesario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CapitalNecesarioSMLV', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FaltanteCapital', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FaltanteSumaAdicional', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'anno', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'mes', 'type':'STRING', 'mode':'NULLABLE'}

        ]
}

table_schema_FactAhorroPensional_malos = {
    'fields': {
        'name': 'AhorroPensionalID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'saldoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SumaAdicional', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CapitalNecesario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CapitalNecesarioSMLV', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FaltanteCapital', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FaltanteSumaAdicional', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'anno', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'mes', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
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
    job_name='factahorropensional-apache-beam-job-name',
    temp_location=config['configService']['temp_location'],
    region=config['configService']['region'],
    service_account_email=config['configService']['service_account_email'],
    save_main_session= config['configService']['save_main_session'])


    table_spec_clean = bigquery.TableReference(
        projectId=config['configService']['project'],
        datasetId='afiliados_pensionados',
        tableId='FactAhorroPensional_clean')


    table_spec_dirty = bigquery.TableReference(
        projectId=config['configService']['project'],
        datasetId='afiliados_pensionados',
        tableId='FactAhorroPensional_dirty')
        
    p = beam.Pipeline(options=options1)        
    FactAhorroPensional  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                    SELECT
                    GENERATE_UUID() as AhorroPensionalID
                    ,a.InfPersonasID
                    ,'None' TiempoID
                    ,CURRENT_DATE() AS FechaDato
                    ,c.SALDO_PENSION_CUOTAS
                    ,'None' SumaAdicional   
                    ,'None' saldoPension
                    ,'None' CapitalNecesario
                    ,'None' CapitalNecesarioSMLV
                    ,'None' FaltanteCapital
                    ,'None' FaltanteSumaAdicional
                    , EXTRACT(YEAR FROM CURRENT_DATE()) as anno
                    , EXTRACT(MONTH FROM CURRENT_DATE()) as mes

                    FROM
                    afiliados-pensionados-prote.afiliados_pensionados.PenFutura c
                    LEFT JOIN
                    afiliados-pensionados-prote.Datamart.dimPensionados j
                    ON
                    j.DocumentoDeLaPersona=c.idAfiliado
                    LEFT JOIN
                    afiliados-pensionados-prote.Datamart.dimInfPersonas a
                    on
                    a.PensionadosId=j.PensionadosId
                ''',\
            use_standard_sql=True))






    FactAhorroPensional_Dict = FactAhorroPensional | beam.Map(lambda x:   {'AhorroPensionalID':str(x['AhorroPensionalID']),\
                                                                          {'TiempoID':str(x['TiempoID']),\
                                                                          {'FechaDato':str(x['FechaDato']),\
                                                                          {'InfPersonasID':str(x['InfPersonasID']),\
                                                                          {'saldoPension':str(x['saldoPension']),\
                                                                          {'SumaAdicional':str(x['SumaAdicional']),\
                                                                          {'CapitalNecesario':str(x['CapitalNecesario']),\
                                                                          {'CapitalNecesarioSMLV':str(x['CapitalNecesarioSMLV']),\
                                                                          {'FaltanteCapital':str(x['FaltanteCapital']),\
                                                                          {'FaltanteSumaAdicional':str(x['FaltanteSumaAdicional']),\
                                                                          {'anno':str(x['anno']),\
                                                                          {'mes':str(x['mes']),\
                                                                           'validacionDetected':""})



    AhorroPensionalID_fullness_validated = FactAhorroPensional_Dict | 'completitud AhorroPensionalID' >> beam.Map(fn_check_completitud,    'AhorroPensionalID' )

    FechaDato_fullness_validated = TiempoID_fullness_validated | 'FechaDato ConsecutivoDeCupon' >> beam.Map(fn_check_completitud,    'FechaDato' )

    InfPersonasID_fullness_validated = FechaDato_fullness_validated | 'InfPersonasID ConsecutivoDeCupon' >> beam.Map(fn_check_completitud,    'InfPersonasID' )

    saldoPension_fullness_validated = InfPersonasID_fullness_validated | 'saldoPension ConsecutivoDeCupon' >> beam.Map(fn_check_completitud,    'saldoPension' )

    saldoPension_numbers_validated = saldoPension_fullness_validated | 'solo numeros saldoPension' >> beam.Map( fn_check_numbers,  'saldoPension')

    saldoPension_biggerThan_validated = saldoPension_numbers_validated | 'bigger than saldoPension' >> beam.Map(fn_check_bigger_than,    'saldoPension', 0)

    anno_fullness_validated = saldoPension_biggerThan_validated | 'anno ConsecutivoDeCupon' >> beam.Map(fn_check_completitud,    'anno' )

    previsional_pendiente_fullness_validated = anno_fullness_validated | 'mes ConsecutivoDeCupon' >> beam.Map(fn_check_completitud,    'mes' )





    results = mes_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x:   {'AhorroPensionalID':str(x['AhorroPensionalID']),\
                                                      {'TiempoID':str(x['TiempoID']),\
                                                      {'FechaDato':str(x['FechaDato']),\
                                                      {'InfPersonasID':str(x['InfPersonasID']),\
                                                      {'saldoPension':str(x['saldoPension']),\
                                                      {'SumaAdicional':str(x['SumaAdicional']),\
                                                      {'CapitalNecesario':str(x['CapitalNecesario']),\
                                                      {'CapitalNecesarioSMLV':str(x['CapitalNecesarioSMLV']),\
                                                      {'FaltanteCapital':str(x['FaltanteCapital']),\
                                                      {'FaltanteSumaAdicional':str(x['FaltanteSumaAdicional']),\
                                                      {'anno':str(x['anno']),\
                                                      {'mes':str(x['mes'])})








    results["validationsDetected"] | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_FactAhorroPensional_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_FactAhorroPensional_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    

    result = p.run()
    result.wait_until_finish()



