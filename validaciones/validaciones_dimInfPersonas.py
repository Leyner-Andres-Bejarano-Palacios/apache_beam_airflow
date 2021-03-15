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
    job_name='diminfpersonas-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='diminfpersonas')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='diminfpersonas_dirty')


  
table_schema_diminfpersonas = {
    'fields': [{
        'name': 'InfPersonasID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'PensionadosId', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'BeneficiariosId', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPersona', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SolicitudesId', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CalculosActuarialesId', 'type':'STRING', 'mode':'NULLABLE'}        
        ]
}

table_schema_diminfpersonas_malos = {
    'fields': [{
        'name': 'InfPersonasID', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'PensionadosId', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'BeneficiariosId', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPersona', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SolicitudesId', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CalculosActuarialesId', 'type':'STRING', 'mode':'NULLABLE'},      
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
            SELECT GENERATE_UUID() as InfPersonasID, BeneficiariosId,TipoPersona,PensionadosId, SolicitudesId, d.Numero_Solicitud, CalculosActuarialesId
            FROM (SELECT a.BeneficiariosId, 'Beneficiario' as TipoPersona, 'None' as PensionadosId, 'None' as SolicitudesId, 'None' as CalculosActuarialesId,
            a.IdentificacionAfiliado
            FROM
            afiliados-pensionados-prote.Datamart.dimBeneficiarios a
            UNION ALL
            SELECT 'None' as BeneficiariosId, 'Cliente' as TipoPersona, PensionadosID,'None' as SolicitudesId, 'None' as CalculosActuarialesId,
            c.DocumentoDeLaPersona
            FROM
            afiliados-pensionados-prote.Datamart.dimPensionados c) c
            LEFT JOIN
            afiliados-pensionados-prote.afiliados_pensionados.PenFutura pen
            ON
            pen.idAfiliado=c.IdentificacionAfiliado

            LEFT JOIN
            afiliados-pensionados-prote.Datamart.factSolicitudes d
            on
            CAST(d.Numero_Solicitud AS FLOAT64)=pen.NUMERO_SOLICITUD
                ''',\
            use_standard_sql=True))






    dimInfPersonas_Dict = dimInfPersonas | beam.Map(lambda x: \
                                                            {'InfPersonasID':str(x['InfPersonasID']),\
                                                             'PensionadosId':str(x['PensionadosId']),\
                                                             'BeneficiariosId':str(x['BeneficiariosId']),\
                                                             'TipoPersona':str(x['TipoPersona']),\
                                                             'SolicitudesId':str(x['SolicitudesId']),\
                                                             'CalculosActuarialesId':str(x['CalculosActuarialesId']),\
                                                             'validacionDetected':""})







#     Sexo_MoF_validated = Sexo_text_validated | 'sexo en valores' >> beam.Map( fn_check_value_in_bene('Sexo',["M","F"]))



#     FechaNacimiento_fullness_validated = Nombre_fullness_validated | 'completitud FechaNacimiento' >> beam.Map(fn_check_completitud('FechaNacimiento'))

#     FechaNacimiento_biggerThan120_validated = FechaNacimiento_fullness_validated | 'nas de 120 annos FechaNacimiento' >> beam.Map( fn_age_less_120('FechaNacimiento'))


#     IdentificacionPension_fullness_validated = FechaNacimiento_biggerThan120_validated | 'completitud IdentificacionPension' >> beam.Map( fn_check_completitud_bene('IdentificacionPension'))

#     IdentificacionPension_numbers_validated = IdentificacionPension_fullness_validated | 'numeros IdentificacionPension' >> beam.Map( fn_check_numbers('IdentificacionPension'))

#     estadoBeneficiario_fullness_validated = IdentificacionPension_numbers_validated | 'completitud estadoBeneficiario' >> beam.Map( fn_check_completitud_bene('estadoBeneficiario'))

#     estadoBeneficiario_text_validated = estadoBeneficiario_fullness_validated | 'solo texto estadoBeneficiario' >> beam.Map( fn_check_text('estadoBeneficiario'))

#     estadoBeneficiario_IoS_validated = Sexo_text_validated | 'estadoBeneficiario en valores' >> beam.Map( fn_check_value_in_bene('estadoBeneficiario',["I","S"]))

#     calidadBeneficiario_completitud_validated = Sexo_text_validated | 'calidadBeneficiario completitud' >> beam.Map( fn_check_completitud_bene('calidadBeneficiario'))

#     TipoBeneficiario_completitud_validated = Sexo_text_validated | 'TipoBeneficiario completitud' >> beam.Map( fn_check_completitud_bene('TipoBeneficiario'))

#     SubtipoBeneficiario_completitud_validated = Sexo_text_validated | 'SubtipoBeneficiario completitud' >> beam.Map( fn_check_completitud_bene('SubtipoBeneficiario'))

#     ParentescoBeneficiario_completitud_validated = Sexo_text_validated | 'ParentescoBeneficiario completitud' >> beam.Map( fn_check_completitud_bene('ParentescoBeneficiario'))

#     ParentescoBeneficiario_text_validated = ParentescoBeneficiario_completitud_validated | 'ParentescoBeneficiario texto' >> beam.Map( fn_check_text('ParentescoBeneficiario'))
    



#    #  regla 22 

#    BeneficiarioMenorAfiliado_text_validated = ParentescoBeneficiario_text_validated | 'Beneficiario menor a afiliado' >> beam.Map( fn_bene_younger_cliient())

#    BeneficiarioMayorAfiliado_text_validated = BeneficiarioMenorAfiliado_text_validated | 'Beneficiario mayor a afiliado' >> beam.Map( fn_bene_older_cliient())

#     #  regla 30

#    numeroRepeticiones_text_validated = BeneficiarioMayorAfiliado_text_validated | 'dos o mas Beneficiarios con misma Id' >> beam.Map( fn_repetead_id("numeroRepeticiones",1))

#    nombreBene_text_validated = numeroRepeticiones_text_validated | 'nombre o apellido de mas de dos caracteres' >> beam.Map( fn_bene_short_name("Nombre",1))

#    EstadoAfiliadoFutura_EstadoAfiliadoApolo_match = nombreBene_text_validated | 'match entre EstadoAfiliadoFutura y EstadoAfiliadoApolo' >> beam.Map( fn_fimd_no_mathcing("EstadoAfiliadoFutura","EstadoAfiliadoApolo"))






    



    results = dimInfPersonas_Dict | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x:         {'InfPersonasID':str(x['InfPersonasID']),\
                                                             'PensionadosId':str(x['PensionadosId']),\
                                                             'BeneficiariosId':str(x['BeneficiariosId']),\
                                                             'TipoPersona':str(x['TipoPersona']),\
                                                             'SolicitudesId':str(x['SolicitudesId']),\
                                                             'CalculosActuarialesId':str(x['CalculosActuarialesId'])})\






    # validadas = results["validationsDetected"] | beam.Map(lambda x: \
    #                                                     {'InformacionPersonasId':str(x['InformacionPersonasId']).encode(encoding = 'utf-8'),\
    #                                                     'FechaCarga':str(x['FechaCarga']).encode(encoding = 'utf-8'),\
    #                                                         'Identificacion':str(x['Identificacion']).encode(encoding = 'utf-8'),\
    #                                                         'tipoIdentificacion':str(x['tipoIdentificacion']).encode(encoding = 'utf-8'),\
    #                                                         'IdentificacionPension':str(x['IdentificacionPension']).encode(encoding = 'utf-8'),\
    #                                                         'Sexo':str(x['Sexo']).encode(encoding = 'utf-8'),\
    #                                                         'Nombre':str(x['Nombre']).encode(encoding = 'utf-8'),\
    #                                                         'estadoBeneficiario':str(x['estadoBeneficiario']).encode(encoding = 'utf-8'),\
    #                                                         'consecutivoPension':str(x['consecutivoPension']).encode(encoding = 'utf-8'),\
    #                                                         'FechaNacimiento':str(x['FechaNacimiento']).encode(encoding = 'utf-8'),\
    #                                                         'SecuenciaBeneficiario':str(x['SecuenciaBeneficiario']).encode(encoding = 'utf-8'),\
    #                                                         'TipoPersona':str(x['TipoPersona']).encode(encoding = 'utf-8'),\
    #                                                         'calidadBeneficiario':str(x['calidadBeneficiario']).encode(encoding = 'utf-8'),\
    #                                                         'TipoBeneficiario':str(x['TipoBeneficiario']).encode(encoding = 'utf-8'),\
    #                                                         'SubtipoBeneficiario':str(x['SubtipoBeneficiario']).encode(encoding = 'utf-8'),\
    #                                                         'fechaVigenciaInicial':str(x['fechaVigenciaInicial']).encode(encoding = 'utf-8'),\
    #                                                         'fechaVigenciaFinal':str(x['fechaVigenciaFinal']).encode(encoding = 'utf-8'),\
    #                                                         'validacionDetected':x['validacionDetected']})







    results["validationsDetected"] | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_diminfpersonas_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_diminfpersonas,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    result = p.run()
    result.wait_until_finish()
