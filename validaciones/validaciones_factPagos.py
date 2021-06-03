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


class ValidadorfactPagosLogic():
    def __init__(self, config):
        self._vName = self.__class__.__name__
        self._vConfig = config

    @staticmethod
    def fn_check_completitud(element,key):
        if (element[key] is None  or str(element[key]).lower() == "none" or (element[key]).lower() == "null" or str(element[key]).replace(" ","") == ""):
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
        return element

    @staticmethod
    def fn_check_numbers(element,key):
        correct = False
        if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None" and str(element[key]).replace(" ","") != ""):
            if (element[key].replace(",","").replace(".","").isnumeric() == True):
                pass
            else:
                correct = True
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no es numerico,"
        return element        


    @staticmethod
    def fn_check_bigger_than(element,key,valueToCompare):
        if (element[key] is not None  and\
            element[key] != "None" and\
            element[key] != "null") and\
            str(element[key]).replace(" ","") != ""  and\
            element[key].strip().replace(",","").replace(".","").isnumeric():
            correct = False
            if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None"):
                if float(element[key]) < float(valueToCompare):
                    element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" es menor que "+str(valueToCompare)+","
        return element



    @staticmethod
    def fn_check_mesada_pensional_0(element,key):
        if (element[key] is not None  and \
            element[key] != "None" and \
            element[key] != "null") and \
           (element["TipoPersona"] is not None  and \
            element["TipoPersona"] != "None" and \
            element["TipoPersona"] != "null"):
            if element["TipoPersona"] == "Cliente":
                if float(element[key]) <= float(0):
                    element["validacionDetected"] = element["validacionDetected"] + "valorPago es menor o igual a 0,"
        return element


    @staticmethod
    def fn_check_mesada_pensional_below(element,key,SMMLV):
        if (element[key] is not None  and \
            element[key] != "None" and \
            element[key] != "null"):
            if element[key].strip().replace(",","").replace(".","").isnumeric():
                if float(str(element[key]).strip().replace(",","").replace(".","")) < float(SMMLV):
                    element["validacionDetected"] = element["validacionDetected"] + "valor de mesada pensional es menor a el SMMLV,"
        return element



    @staticmethod      
    def fn_check_date_format(element,key,dateFormat):
        if (element[key] is not None  and element[key] != "None" and element[key] != "null" and str(element[key]).replace(" ","") != ""): 
            try:
                datetime.strptime(element[key].replace(" ",""), dateFormat)
            except:
                element["validacionDetected"] = element["validacionDetected"] + str(key) +" tiene un formato de fecha invalida," 
            finally:
                return element
        else:
            return element


    @staticmethod
    def fn_check_lesser_than(element,key,valueToCompare):
        if (element[key] is not None  and \
            element[key] != "None" and \
            element[key] != "null"):
            if  float(str(element[key]).strip().replace(",","").replace(".","")) > \
                float(str(valueToCompare).strip().replace(",","").replace(".","")):
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" es mayor que "+str(valueToCompare)+","
        return element



table_schema_factPagos = {
    'fields': [{
        'name':'PagosID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ConceptoPagoOrigen', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ConceptoPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoPagoID', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},    
        {
        'name':'ValorPagoAnterior', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'ValorPago', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'FechaGeneracionPago', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'FechaDePago', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'periodo', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_factPagos_malos = {
    'fields': [{
        'name':'PagosID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ConceptoPagoOrigen', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ConceptoPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoPagoID', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},    
        {
        'name':'ValorPagoAnterior', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'ValorPago', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'FechaGeneracionPago', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'FechaDePago', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'periodo', 'type':'STRING', 'mode':'NULLABLE'},                                   
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}


config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
validador = ValidadorfactPagosLogic(config)
options1 = PipelineOptions(
argv= None,
runner=config['configService']['runner'],
project=config['configService']['project'],
job_name='factpagos-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_factPagos')


table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_factpagos_dirty')


with beam.Pipeline(options=options1) as p:   
    dimInfPersonas  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                    WITH PAGOS AS (
                    (SELECT GENERATE_UUID() AS PagosID,
                    'None' TiempoID,
                    'None' Inf_personasID,
                    TIPO_PENSION TipoPension,
                    CONCEPTO_PAGO ConceptoPago,
                    ESTADO_PAGO EstadoPagoID,
                    CURRENT_DATE() AS FechaDato,
                    CAST(VALOR_MESADA_ANTERIOR AS STRING) ValorPagoAnterior,
                    VALOR_PAGO ValorPago,
                    FECHA_GENERACION_PAGO FechaGeneracionPago,
                    FECHA_PAGO FechaDePago,
                    'None'periodo,
                    case when VALOR_MESADA_ANTERIOR IS null and VALOR_PAGO is null then 'None'  
                    else 'MesadaActual'
                    end as ConceptoPagoOrigen, pen.AFI_HASH64
                    FROM 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_pensionadosFutura pen
                    LEFT JOIN
                    `'''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_pagos` pa
                    ON
                    pa.AFI_HASH64=pen.AFI_HASH64)

                    UNION DISTINCT 
                    (
                    SELECT GENERATE_UUID() AS PagosID,
                    'None' TiempoID,
                    'None' Inf_personasID,
                    'None' TipoPension,
                    'RetiroSaldos' ConceptoPago,
                    ret.ESTADO_PAGO EstadoPagoID,
                    CURRENT_DATE() AS FechaDato,
                    'None' ValorPagoAnterior,
                    ret.VALOR_PAGO ValorPago,
                    CAST(ret.FECHA_GENERACION_PAGO AS STRING) FechaGeneracionPago,
                    CAST(ret.FECHA_PAGO AS STRING) FechaDePago,
                    'None'periodo,
                    case when ret.Valor_Pago is null 
                    then 'None'  
                    else 'MesadaActual'
                    end as ConceptoPagoOrigen, ret.AFI_HASH64
                    FROM 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Retiros ret
                    LEFT JOIN
                    `'''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_pagos` pa
                    ON
                    pa.AFI_HASH64=ret.AFI_HASH64    
                    )
                    UNION DISTINCT 
                    (
                    SELECT GENERATE_UUID() AS PagosID,
                    'None' TiempoID,
                    'None' Inf_personasID,
                    'None' TipoPensionID,
                    'DevolucionSaldos' ConceptoPago,
                    ESTADO_PAGO EstadoPagoID,
                    CURRENT_DATE() AS FechaDato,
                    'None' ValorPagoAnterior,
                    dev.MONTO_DEVOLUCION ValorPago,
                    FECHA_GENERACION_PAGO FechaGeneracionPago,
                    CAST(dev.FECHA_DEVOLUCION AS STRING) FechaDePago,
                    'None'periodo,
                    case when MONTO_DEVOLUCION is null 
                    then 'None'  
                    else 'ValorDevolucion' end as ConceptoPagoOrigen, dev.AFI_HASH64
                    FROM 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_DMP_CYR_DEVOLUCION_SALDOS dev
                    LEFT JOIN
                    `'''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_pagos` pa
                    ON
                    pa.AFI_HASH64=dev.AFI_HASH64    
                    ))
                    SELECT PagosID,TiempoID,info.InfPersonasID,TipoPension,ConceptoPago,EstadoPagoID,FechaDato,ValorPagoAnterior,
                    ValorPago,FechaGeneracionPago,FechaDePago,periodo,ConceptoPagoOrigen
                    FROM PAGOS

                    LEFT JOIN 

                    ((SELECT InfPersonasID, dpen.AFI_HASH64 FROM 
                    `'''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_dimInfPersonas` inf
                    LEFT JOIN 
                    (SELECT DISTINCT AFI_HASH64,PensionadosID 
                    FROM 
                    '''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_dimPensionados
                    ) dpen
                    ON 
                    dpen.PensionadosID=inf.PensionadosID)

                    UNION DISTINCT 

                    (
                    SELECT InfPersonasID, dben.AFI_HASH64 FROM 
                    `'''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_dimInfPersonas` inf
                    LEFT JOIN
                    (SELECT DISTINCT AFI_HASH64, BeneficiariosID 
                    from '''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_dimBeneficiarios
                    ) dben
                    ON 
                    dben.BeneficiariosId=inf.BeneficiariosId
                    )
                    ) info
                    ON 
                    info.AFI_HASH64=PAGOS.AFI_HASH64

                ''',\
            use_standard_sql=True))






    dimInfPersonas_Dict = dimInfPersonas | beam.Map(lambda x: \
                                                            {'PagosID':str(x['PagosID']),\
                                                            'TiempoID':str(x['TiempoID']),\
                                                            'InfPersonasID':str(x['InfPersonasID']),\
                                                            'TipoPension':str(x['TipoPension']),\
                                                            'ConceptoPago':str(x['ConceptoPago']),\
                                                            'EstadoPagoID':str(x['EstadoPagoID']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'ValorPagoAnterior':str(x['ValorPagoAnterior']),\
                                                            'ValorPago':str(x['ValorPago']),\
                                                            'FechaGeneracionPago':str(x['FechaGeneracionPago']),\
                                                            'FechaDePago':str(x['FechaDePago']),\
                                                            'periodo':str(x['periodo']),\
                                                            'ConceptoPagoOrigen':str(x['ConceptoPagoOrigen']),\
                                                            'validacionDetected':""})


    TipoPension_fullness_validated = dimInfPersonas_Dict | 'completitud TipoPension' >> beam.Map(validador.fn_check_completitud,    'TipoPension' )

    ConceptoPago_fullness_validated = TipoPension_fullness_validated | 'completitud ConceptoPago' >> beam.Map(validador.fn_check_completitud,    'ConceptoPago' )

    EstadoPago_fullness_validated = ConceptoPago_fullness_validated | 'completitud EstadoPago' >> beam.Map(validador.fn_check_completitud,    'EstadoPagoID' )

    FechaDato_fullness_validated = EstadoPago_fullness_validated | 'completitud FechaDato' >> beam.Map(validador.fn_check_completitud,    'FechaDato' )

    ValorPagoAnterior_fullness_validated = FechaDato_fullness_validated | 'completitud ValorPagoAnterior' >> beam.Map(validador.fn_check_completitud,    'ValorPagoAnterior' )

    ValorPagoAnterior_numbers_validated = ValorPagoAnterior_fullness_validated | 'solo numeros ValorPagoAnterior' >> beam.Map( validador.fn_check_numbers,  'ValorPagoAnterior')

    ValorPagoAnterior_biggerThan_validated = ValorPagoAnterior_numbers_validated | 'bigger than ValorPagoAnterior' >> beam.Map(validador.fn_check_bigger_than,    'ValorPagoAnterior',0 )

    ValorPago_fullness_validated = ValorPagoAnterior_biggerThan_validated | 'completitud ValorPago' >> beam.Map(validador.fn_check_completitud,    'ValorPago' )

    ValorPago_biggerThan_validated = ValorPago_fullness_validated | 'bigger than ValorPago' >> beam.Map(validador.fn_check_bigger_than,    'ValorPago',0 )

    ValorPago_numbers_validated = ValorPago_biggerThan_validated | 'solo numeros ValorPagoAnterior1' >> beam.Map( validador.fn_check_numbers,  'ValorPago')

    FechaDePago_fullness_validated = ValorPago_numbers_validated | 'completitud FechaDePago' >> beam.Map(validador.fn_check_completitud,    'FechaDePago' )


    # NombreDestinatarioPago_fullness_validated = FechaDePago_fullness_validated | 'completitud NombreDestinatarioPago' >> beam.Map(validador.fn_check_completitud,    'NombreDestinatarioPago' )

    # TipoIdentificacionDestinatarioPago_fullness_validated = NombreDestinatarioPago_fullness_validated | 'completitud TipoIdentificacionDestinatarioPago' >> beam.Map(validador.fn_check_completitud,    'TipoIdentificacionDestinatarioPago' )

    # IdentificacioDestinatarioPago_fullness_validated = TipoIdentificacionDestinatarioPago_fullness_validated | 'completitud IdentificacioDestinatarioPago' >> beam.Map(validador.fn_check_completitud,    'IdentificacioDestinatarioPago' )

    # TipoPersona_fullness_validated = FechaDePago_fullness_validated | 'completitud TipoPersona' >> beam.Map(validador.fn_check_completitud,    'TipoPersona' )

    # ValorPago_QualityRule_2_checked = TipoPersona_fullness_validated | 'validando regla 2 mesada pensional mayor a cero' >> beam.Map(validador.fn_check_mesada_pensional_0,    'ValorPago' )

    ValorPago_QualityRule_4_checked = FechaDePago_fullness_validated | 'validando regla 4 mesada pensional menor a SMMLV' >> beam.Map(validador.fn_check_mesada_pensional_below,    'ValorPago', config['DEFAULT']['SMMLV'] )

    FechaGeneracionPago_fullness_validated = ValorPago_QualityRule_4_checked | 'completitud FechaGeneracionPago' >> beam.Map(validador.fn_check_completitud,    'FechaGeneracionPago' )

    FechaGeneracionPago_dateFormat_validated = FechaGeneracionPago_fullness_validated | 'date format FechaGeneracionPago' >> beam.Map(validador.fn_check_date_format,    'FechaGeneracionPago', "%Y%m%d" )

    FechaGeneracionPago_biggerThan_validated = FechaGeneracionPago_dateFormat_validated | 'bigger than FechaGeneracionPago' >> beam.Map(validador.fn_check_bigger_than,    'FechaGeneracionPago', 19940401)

    FechaGeneracionPago_QualityRule_7_checked = FechaGeneracionPago_biggerThan_validated  | 'validando regla 7 partially compltitud' >> beam.Map(validador.fn_check_completitud,    'FechaGeneracionPago')

    ValorPago_QualityRule_48_checked = FechaGeneracionPago_QualityRule_7_checked | 'validando regla 48 mesada pensional muy alta' >> beam.Map(validador.fn_check_lesser_than,    'ValorPago', config['factPagos']['MesadasPensionalesMuyAltas'])

  



    results = ValorPago_QualityRule_48_checked | beam.ParDo(fn_divide_clean_dirty()).with_outputs()



    limpias = results["Clean"] | beam.Map(lambda x: \
                                                            {'PagosID':str(x['PagosID']),\
                                                            'TiempoID':str(x['TiempoID']),\
                                                            'InfPersonasID':str(x['InfPersonasID']),\
                                                            'TipoPension':str(x['TipoPension']),\
                                                            'ConceptoPago':str(x['ConceptoPago']),\
                                                            'ConceptoPagoOrigen':str(x['ConceptoPagoOrigen']),\
                                                            'EstadoPagoID':str(x['EstadoPagoID']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'ValorPagoAnterior':str(x['ValorPagoAnterior']),\
                                                            'ValorPago':str(x['ValorPago']),\
                                                            'FechaGeneracionPago':str(x['FechaGeneracionPago']),\
                                                            'FechaDePago':str(x['FechaDePago']),\
                                                            'periodo':str(x['periodo'])})


    validadas = results["validationsDetected"] | beam.Map(lambda x: \
                                                            {'PagosID':str(x['PagosID']),\
                                                            'TiempoID':str(x['TiempoID']),\
                                                            'InfPersonasID':str(x['InfPersonasID']),\
                                                            'TipoPension':str(x['TipoPension']),\
                                                            'ConceptoPago':str(x['ConceptoPago']),\
                                                            'ConceptoPagoOrigen':str(x['ConceptoPagoOrigen']),\
                                                            'EstadoPagoID':str(x['EstadoPagoID']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'ValorPagoAnterior':str(x['ValorPagoAnterior']),\
                                                            'ValorPago':str(x['ValorPago']),\
                                                            'FechaGeneracionPago':str(x['FechaGeneracionPago']),\
                                                            'FechaDePago':str(x['FechaDePago']),\
                                                            'periodo':str(x['periodo']),\
                                                            'validacionDetected':str(x['validacionDetected'])})



    validadas | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_factPagos_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_factPagos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

