import uuid
import time
import types
import threading
import numpy as np
import pandas as pd
import configparser
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.runners.runner import PipelineState
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions





options1 = PipelineOptions(
    argv= None,
    runner='DataflowRunner',
    project='afiliados-pensionados-prote',
    job_name='factpagos-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='factPagos')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='factpagos_dirty')


  
table_schema_factPagos = {
    'fields': [
        {'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPensionID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ConceptoPagoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ValorPagoAnterior', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ValorPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaGeneracionPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Fecha_Pago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'anno', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'mes', 'type':'STRING', 'mode':'NULLABLE'    
        },
        {
        'name':'NombreDestinatarioPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoIdentificacionDestinatarioPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IdentificacioDestinatarioPago', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_factPagos_malos = {
    'fields': [{
        'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPensionID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ConceptoPagoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ValorPagoAnterior', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ValorPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaGeneracionPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Fecha_Pago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'anno', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'mes', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'NombreDestinatarioPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoIdentificacionDestinatarioPago', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IdentificacioDestinatarioPago', 'type':'STRING', 'mode':'NULLABLE'},
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


def fn_check_numbers(element,key):
    if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None"):
        try:
            float(element[key])
            pass
        except :
            element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no son numeros,"
    return element


def fn_check_bigger_than(element,key,valueToCompare):
    if (element[key] is not None  and element[key] != "None" and element[key] != "null" and str(element[key]).strip().isnumeric()):
        try:
            correct = False
            cedulaNumerica = float(element[key].strip())
            if  cedulaNumerica < float(valueToCompare):
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" es menor que "+str(valueToCompare)+","
        finally:
            pass
    return element



def fn_check_mesada_pensional_0(element,key):
    if (element[key] is None  or element[key] == "None" or element[key] == "null") and \
       (element["TipoPersona"] is None  or element["TipoPersona"] == "None" or element["TipoPersona"] == "null"):
        if element["TipoPersona"] == "Cliente":
            if float(element[key]) <= float(0):
                element["validacionDetected"] = element["validacionDetected"] + "valor de mesada pensional es menor o igual a 0,"
    return element


def fn_check_mesada_high(element,key,compareTo):
    if (element[key] is None  and element[key] != "None" and element[key] != "null"):
        if element[key].isnumeric():
            if float(element[key]) > float(compareTo):
                element["validacionDetected"] = element["validacionDetected"] + "valor de mesada pensional es menor a el SMMLV,"
    return element


def fn_check_date_format(element,key,dateFormat):
    if (element[key] is not None  or element[key] != "None" or element[key] != "null"): 
        try:
            datetime.strptime(element[key], dateFormat)
        except:
            element["validacionDetected"] = element["validacionDetected"] + str(key) +" tiene un formato de fecha invalida," 
        finally:
            return element
    else:
        return element


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('/home/airflow/gcs/data/config.ini')
    p = beam.Pipeline(options=options1)
    dimInfPersonas  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                    -- // 4------NombreDestinatarioPago, validarlo con Maria (sospechamos que es el beneficiario o el pensionado)
                        SELECT TiempoID,a.InfPersonasID,a.TipoPensionID,
                        ConceptoPagoID,FechaDato,ValorPago,ValorPagoAnterior,
                        FechaGeneracionPago ,Fecha_Pago,anno, mes,NombreDestinatarioPago,TipoIdentificacionDestinatarioPago,IdentificacioDestinatarioPago,
                        EstadoPago, a.TipoPersona

                        FROM(SELECT TiempoID, InfPersonasID,TipoPensionID,ConceptoPagoID,FechaDato, ValorPago,
                        ValorPagoAnterior,PAGFE6 as FechaGeneracionPago ,Fecha_Pago,anno, mes,NombreDestinatarioPago,TipoIdentificacionDestinatarioPago,IdentificacioDestinatarioPago,
                        'None' EstadoPago, TipoPersona
                        FROM(SELECT DISTINCT e.idAfiliado, "None" TiempoID, a.InfPersonasID, b.TipoPensionID, xx.ConceptoPagoID, /*d.EstadoPago,*/ current_date() FechaDato, e.NUMERO_MESADAS as Mesadas, VALOR_MEDADA_ACTUAL AS ValorPago, VALOR_MESADA_ANTERIOR as ValorPagoAnterior, r.PAGFE6, Fecha_Pago,extract(year from current_date()) as anno, extract(month from current_date()) as mes, 'None' NombreDestinatarioPago,
                        'None' TipoIdentificacionDestinatarioPago, 'None' IdentificacioDestinatarioPago, f.TipoPersona
                        FROM
                        afiliados-pensionados-prote.afiliados_pensionados.PenFutura e
                        LEFT JOIN
                        (SELECT PensionadosID, DocumentoDeLaPersona, 'Cliente' TipoPersona FROM
                        afiliados-pensionados-prote.Datamart.dimPensionados c
                        UNION ALL
                        SELECT BeneficiariosID, IdentificacionAfiliado, 'Beneficiario' TipoPersona FROM
                        afiliados-pensionados-prote.Datamart.dimBeneficiarios d
                        ) f
                        ON
                        e.idAfiliado=f.DocumentoDeLaPersona
                        LEFT JOIN
                        afiliados-pensionados-prote.Datamart.dimInfPersonas a
                        ON
                        f.PensionadosId=a.PensionadosId
                        LEFT JOIN
                        afiliados-pensionados-prote.Datamart.dimTipoPension b
                        ON
                        b.TipoPension=e.TIPO_PENSION
                        LEFT JOIN
                        notional-radio-302217.DatalakeAnalitica.SQLSERVER_BIPROTECCIONDW_DW_DIMAFILIADOS_Z51 x
                        ON
                        x.idAfiliado=e.idAfiliado
                        LEFT JOIN
                        (SELECT NUMERO_MESADAS, idAfiliado, case when NUMERO_MESADAS <> 'null' then 'Mesada actual' end as ConceptoPago,
                        FROM(select idAfiliado, cast(NUMERO_MESADAS AS STRING) AS NUMERO_MESADAS
                        from
                        afiliados-pensionados-prote.afiliados_pensionados.FuturaPensionados)) c
                        ON
                        c.idAfiliado=e.idAfiliado
                        LEFT JOIN
                        afiliados-pensionados-prote.Datamart.dimConceptoPago xx
                        ON
                        xx.ConceptoPago=c.ConceptoPago
                        LEFT JOIN
                        afiliados-pensionados-prote.afiliados_pensionados.tabla_fed_retiros_futura r
                        on
                        r.afi_hash64=x.afi_hash64
                        left join
                        afiliados-pensionados-prote.afiliados_pensionados.DevolucionesPAGAR2 z
                        ON
                        z.IDENTIFICACION=e.idAfiliado
                        )) a
                        LEFT JOIN
                        afiliados-pensionados-prote.Datamart.dimInfPersonas b
                        ON
                        a.InfPersonasID=b.InfPersonasID

                ''',\
            use_standard_sql=True))






    dimInfPersonas_Dict = dimInfPersonas | beam.Map(lambda x: \
                                                            {'TiempoID':str(x['TiempoID']),\
                                                            'InfPersonasID':str(x['InfPersonasID']),\
                                                            'TipoPensionID':str(x['TipoPensionID']),\
                                                            'ConceptoPagoID':str(x['ConceptoPagoID']),\
                                                            'EstadoPago':str(x['EstadoPago']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'ValorPagoAnterior':str(x['ValorPagoAnterior']),\
                                                            'ValorPago':str(x['ValorPago']),\
                                                            'FechaGeneracionPago':str(x['FechaGeneracionPago']),\
                                                            'Fecha_Pago':str(x['Fecha_Pago']),\
                                                            'anno':str(x['anno']),\
                                                            'mes':str(x['mes']),\
                                                            'NombreDestinatarioPago':str(x['NombreDestinatarioPago']),\
                                                            'TipoIdentificacionDestinatarioPago':str(x['TipoIdentificacionDestinatarioPago']),\
                                                            'IdentificacioDestinatarioPago':str(x['IdentificacioDestinatarioPago']),\
                                                            'TipoPersona':str(x['TipoPersona']),\
                                                            'validacionDetected':""})


    TiempoID_fullness_validated = dimInfPersonas_Dict | 'completitud TiempoID' >> beam.Map(fn_check_completitud,    'TiempoID' )

    InfPersonasID_fullness_validated = TiempoID_fullness_validated | 'completitud InfPersonasID' >> beam.Map(fn_check_completitud,    'InfPersonasID' )

    TipoPensionID_fullness_validated = InfPersonasID_fullness_validated | 'completitud TipoPensionID' >> beam.Map(fn_check_completitud,    'TipoPensionID' )

    ConceptoPagoID_fullness_validated = TipoPensionID_fullness_validated | 'completitud ConceptoPagoID' >> beam.Map(fn_check_completitud,    'ConceptoPagoID' )

    EstadoPago_fullness_validated = ConceptoPagoID_fullness_validated | 'completitud EstadoPago' >> beam.Map(fn_check_completitud,    'EstadoPago' )

    FechaDato_fullness_validated = EstadoPago_fullness_validated | 'completitud FechaDato' >> beam.Map(fn_check_completitud,    'FechaDato' )

    ValorPagoAnterior_fullness_validated = FechaDato_fullness_validated | 'completitud ValorPagoAnterior' >> beam.Map(fn_check_completitud,    'ValorPagoAnterior' )

    ValorPagoAnterior_numbers_validated = ValorPagoAnterior_fullness_validated | 'solo numeros ValorPagoAnterior' >> beam.Map( fn_check_numbers,  'ValorPagoAnterior')

    ValorPagoAnterior_biggerThan_validated = ValorPagoAnterior_numbers_validated | 'bigger than ValorPagoAnterior' >> beam.Map(fn_check_bigger_than,    'ValorPagoAnterior',0 )

    ValorPago_fullness_validated = ValorPagoAnterior_biggerThan_validated | 'completitud ValorPago' >> beam.Map(fn_check_completitud,    'ValorPago' )

    ValorPago_biggerThan_validated = ValorPago_fullness_validated | 'bigger than ValorPago' >> beam.Map(fn_check_bigger_than,    'ValorPago',0 )

    ValorPago_numbers_validated = ValorPago_biggerThan_validated | 'solo numeros ValorPagoAnterior1' >> beam.Map( fn_check_numbers,  'ValorPago')

    Fecha_Pago_fullness_validated = ValorPago_numbers_validated | 'completitud Fecha_Pago' >> beam.Map(fn_check_completitud,    'Fecha_Pago' )


    NombreDestinatarioPago_fullness_validated = Fecha_Pago_fullness_validated | 'completitud NombreDestinatarioPago' >> beam.Map(fn_check_completitud,    'NombreDestinatarioPago' )

    TipoIdentificacionDestinatarioPago_fullness_validated = NombreDestinatarioPago_fullness_validated | 'completitud TipoIdentificacionDestinatarioPago' >> beam.Map(fn_check_completitud,    'TipoIdentificacionDestinatarioPago' )

    IdentificacioDestinatarioPago_fullness_validated = TipoIdentificacionDestinatarioPago_fullness_validated | 'completitud IdentificacioDestinatarioPago' >> beam.Map(fn_check_completitud,    'IdentificacioDestinatarioPago' )

    TipoPersona_fullness_validated = IdentificacioDestinatarioPago_fullness_validated | 'completitud TipoPersona' >> beam.Map(fn_check_completitud,    'TipoPersona' )

    ValorPago_QualityRule_2_checked = TipoPersona_fullness_validated | 'validando regla 2 mesada pensional mayor a cero' >> beam.Map(fn_check_mesada_pensional_0,    'ValorPago' )

    ValorPago_QualityRule_4_checked = ValorPago_QualityRule_2_checked | 'validando regla 4 mesada pensional menor a SMMLV' >> beam.Map(fn_check_mesada_pensional_below,    'ValorPago', config['DEFAULT']['SMMLV'] )

    FechaGeneracionPago_fullness_validated = ValorPago_QualityRule_4_checked | 'completitud FechaGeneracionPago' >> beam.Map(fn_check_completitud,    'FechaGeneracionPago' )

    FechaGeneracionPago_dateFormat_validated = FechaGeneracionPago_fullness_validated | 'date format FechaGeneracionPago' >> beam.Map(fn_check_date_format,    'FechaGeneracionPago', "%Y%m%d" )

    FechaGeneracionPago_biggerThan_validated = FechaGeneracionPago_dateFormat_validated | 'bigger than FechaGeneracionPago' >> beam.Map(fn_check_bigger_than,    'FechaGeneracionPago', 19940401)

    FechaGeneracionPago_QualityRule_7_checked = FechaGeneracionPago_biggerThan_validated  | 'validando regla 7 partially compltitud' >> beam.Map(fn_check_completitud,    'FechaGeneracionPago')

    ValorPago_QualityRule_48_checked = FechaGeneracionPago_QualityRule_7_checked | 'validando regla 48 mesada pensional muy alta' >> beam.Map(fn_check_mesada_high,    'ValorPago', config['factPagos']['MesadasPensionalesMuyAltas'])

  



    results = ValorPago_QualityRule_48_checked | beam.ParDo(fn_divide_clean_dirty()).with_outputs()



    limpias = results["Clean"] | beam.Map(lambda x:         {'TiempoID':str(x['TiempoID']),\
                                                            'InfPersonasID':str(x['InfPersonasID']),\
                                                            'TipoPensionID':str(x['TipoPensionID']),\
                                                            'ConceptoPagoID':str(x['ConceptoPagoID']),\
                                                            'EstadoPago':str(x['EstadoPago']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'ValorPagoAnterior':str(x['ValorPagoAnterior']),\
                                                            'ValorPago':str(x['ValorPago']),\
                                                            'FechaGeneracionPago':str(x['FechaGeneracionPago']),\
                                                            'Fecha_Pago':str(x['Fecha_Pago']),\
                                                            'anno':str(x['anno']),\
                                                            'mes':str(x['mes']),\
                                                            'NombreDestinatarioPago':str(x['NombreDestinatarioPago']),\
                                                            'TipoIdentificacionDestinatarioPago':str(x['TipoIdentificacionDestinatarioPago']),\
                                                            'IdentificacioDestinatarioPago':str(x['IdentificacioDestinatarioPago'])})


    validadas = results["validationsDetected"] | beam.Map(lambda x: {'TiempoID':str(x['TiempoID']),\
                                                            'InfPersonasID':str(x['InfPersonasID']),\
                                                            'TipoPensionID':str(x['TipoPensionID']),\
                                                            'ConceptoPagoID':str(x['ConceptoPagoID']),\
                                                            'EstadoPago':str(x['EstadoPago']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'ValorPagoAnterior':str(x['ValorPagoAnterior']),\
                                                            'ValorPago':str(x['ValorPago']),\
                                                            'FechaGeneracionPago':str(x['FechaGeneracionPago']),\
                                                            'Fecha_Pago':str(x['Fecha_Pago']),\
                                                            'anno':str(x['anno']),\
                                                            'mes':str(x['mes']),\
                                                            'NombreDestinatarioPago':str(x['NombreDestinatarioPago']),\
                                                            'TipoIdentificacionDestinatarioPago':str(x['TipoIdentificacionDestinatarioPago']),\
                                                            'IdentificacioDestinatarioPago':str(x['IdentificacioDestinatarioPago']),\
                                                            'validacionDetected':str(x['validacionDetected']}))



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
    result = p.run()
    result.wait_until_finish()