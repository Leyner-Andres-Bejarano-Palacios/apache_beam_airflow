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
    job_name='factdefiniciones-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='factDefiniciones')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='factDefiniciones_dirty')




table_schema_factDefiniciones = {
    'fields': [
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPensionID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaModificacion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasMomentoDefinicion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasProteccion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasAFP', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasBono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'modalidadPension', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_factDefiniciones_malos = {
    'fields': [
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPensionID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaModificacion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasMomentoDefinicion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasProteccion', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasAFP', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SemanasBono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'modalidadPension', 'type':'STRING', 'mode':'NULLABLE'},
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


def fn_check_bigger_than(element,key,valueToCompare):
    correct = False
    if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None"):
        if element[key].isnumeric():
            if float(element[key]) < float(valueToCompare):
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" es menor que "+str(valueToCompare)+","
        else:
            element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no es numerico,"
    return element


def fn_check_smaller_than(element,key,valueToCompare):
    correct = False
    if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None"):
        if element[key].isnumeric():
            if float(element[key]) > float(valueToCompare):
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" es menor que "+str(valueToCompare)+","
        else:
            element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no es numerico,"
    return element

def fn_check_text(element,key):
    if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None"):
        if (str(element[key]).replace(" ","").isalpha() == False):
            element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no son texto,"
    return element

def fn_check_pension_less_than_50(element):
    if (element["TipoPension"] != "None" and \
        element["SemanasMomentoDefinicion"] != "None" and \
        str(element["SemanasMomentoDefinicion"]).strip().isnumeric() and \
        element["SemanasMomentoDefinicion"] is not None):
        if element["TipoPension"].strip() == "2" or element["TipoPension"].strip() == "3":
            if float(element["SemanasMomentoDefinicion"]) < float(50):
                element["validacionDetected"] = element["validacionDetected"] + "persona con pension de invalidez o sobrevivencia con menos de 50 en campo total_semanas - semanasAlMomentoDeLaDefinicion,"
    return element


if __name__ == "__main__":
    p = beam.Pipeline(options=options1)
    factDefiniciones  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                -- // se nos pide ingestar semanas_afp, tomar este campo de semanas_otras_afp (ignorar )

                -- // lo mismo con semanas protecion, y no traiga el IBL (niguno de los dos)

                -- // semanas bono tampoco lo tome (ninguno de los dos)

                -- // semanasAlMomento de definicion (validarlo con Nataly y Juan David, lo mas probable es que sea total_semanas) '-----> si es lo mismo

                SELECT c.InfPersonasID, b.TipoPensionID, b.TipoPension, CURRENT_DATE() as /*Validar si es fecha del dato*/ FechaModificacion, TOTAL_SEMANAS as /*Validar si es TOTAL_SEMANAS*/ SemanasMomentoDefinicion, SEMANAS_PROTECCION as SemanasProteccion, a.SEMANAS_OTRAS_AFP /*Validar si es SEMANAS_OTRAS_AFP o SEMANAS_OTRAS_AFP_FECHA_SINIESTRO*/ as SemanasAFP, a.SEMANAS_BONO1 as /*Validar si es SEMANAS_BONO1 o SEMANAS_BONO2*/ SemanasBono,
                pen.MODALIDAD modalidadPension
                FROM
                afiliados-pensionados-prote.afiliados_pensionados.tabla_fed_Advance a
                LEFT JOIN
                (SELECT DISTINCT idAfiliado, TIPO_PENSION,MODALIDAD FROM
                afiliados-pensionados-prote.afiliados_pensionados.PenFutura pen
                ) pen
                ON
                pen.idAfiliado=a.idAfiliado
                LEFT JOIN
                afiliados-pensionados-prote.Datamart.dimTipoPension b
                ON
                b.TipoPension=pen.TIPO_PENSION
                LEFT JOIN
                (select distinct ben.IdentificacionAfiliado FROM
                afiliados-pensionados-prote.Datamart.dimBeneficiarios ben
                ) ben
                ON
                ben.IdentificacionAfiliado=pen.idAfiliado
                LEFT JOIN
                (select distinct DocumentoDeLaPersona, PensionadosID from
                afiliados-pensionados-prote.Datamart.dimPensionados m
                ) m
                ON
                pen.idAfiliado=m.DocumentoDeLaPersona
                LEFT JOIN
                afiliados-pensionados-prote.Datamart.dimInfPersonas c
                ON
                c.PensionadosId = m.PensionadosID
                                ''',\
            use_standard_sql=True))






    factDefiniciones_Dict = factDefiniciones | beam.Map(lambda x: \
                                                            {'InfPersonasID':str(x['InfPersonasID']),\
                                                            'TipoPensionID':str(x['TipoPensionID']),\
                                                            'TipoPension':str(x['TipoPension']),\
                                                            'SemanasMomentoDefinicion':str(x['SemanasMomentoDefinicion']),\
                                                            'SemanasProteccion':str(x['SemanasProteccion']),\
                                                            'SemanasAFP':str(x['SemanasAFP']),\
                                                            'SemanasBono':str(x['SemanasBono']),\
                                                            'modalidadPension':str(x['modalidadPension']),\
                                                            'validacionDetected':""})


    InfPersonasID_fullness_validated = factDefiniciones_Dict | 'completitud InfPersonasID' >> beam.Map(fn_check_completitud,    'InfPersonasID' )

    TipoPensionID_fullness_validated = InfPersonasID_fullness_validated | 'completitud TipoPensionID' >> beam.Map(fn_check_completitud,    'TipoPensionID' )

    SemanasMomentoDefinicion_fullness_validated = TipoPensionID_fullness_validated | 'completitud SemanasMomentoDefinicion' >> beam.Map(fn_check_completitud,    'SemanasMomentoDefinicion' )

    SemanasMomentoDefinicion_biggerThan_validated = SemanasMomentoDefinicion_fullness_validated | 'bigger than SemanasMomentoDefinicion' >> beam.Map(fn_check_bigger_than,    'SemanasMomentoDefinicion',0 )

    SemanasMomentoDefinicion_QualityRule_58 = SemanasMomentoDefinicion_biggerThan_validated | 'smaller than SemanasMomentoDefinicion' >> beam.Map(fn_check_smaller_than,    'SemanasMomentoDefinicion',1150 )  

    SemanasBono_validated = SemanasMomentoDefinicion_QualityRule_58 | 'completitud SemanasBono' >> beam.Map(fn_check_completitud,    'SemanasBono' )

    SemanasAFP_validated = SemanasBono_validated | 'completitud SemanasAFP' >> beam.Map(fn_check_completitud,    'SemanasAFP' )

    SemanasProteccion_validated = SemanasAFP_validated | 'completitud SemanasProteccion' >> beam.Map(fn_check_completitud,    'SemanasProteccion' )

    SemanasMomentoDefinicio_QualityRule_19 = SemanasProteccion_validated | 'pension invalidez or survivor with less than 50 total weeks' >> beam.Map(fn_check_pension_less_than_50) 

    










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






    



    results = SemanasMomentoDefinicio_QualityRule_19 | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x: \
                                                            {'InfPersonasID':str(x['InfPersonasID']),\
                                                            'TipoPensionID':str(x['TipoPensionID']),\
                                                            'TipoPension':str(x['TipoPension']),\
                                                            'SemanasMomentoDefinicion':str(x['SemanasMomentoDefinicion']),\
                                                            'SemanasProteccion':str(x['SemanasProteccion']),\
                                                            'SemanasAFP':str(x['SemanasAFP']),\
                                                            'SemanasBono':str(x['SemanasBono']),\
                                                            'modalidadPension':str(x['modalidadPension'])})                                                                                      





    dirty = results["validationsDetected"] | beam.Map(lambda x: \
                                                            {'InfPersonasID':str(x['InfPersonasID']),\
                                                            'TipoPensionID':str(x['TipoPensionID']),\
                                                            'TipoPension':str(x['TipoPension']),\
                                                            'SemanasMomentoDefinicion':str(x['SemanasMomentoDefinicion']),\
                                                            'SemanasProteccion':str(x['SemanasProteccion']),\
                                                            'SemanasAFP':str(x['SemanasAFP']),\
                                                            'SemanasBono':str(x['SemanasBono']),\
                                                            'modalidadPension':str(x['modalidadPension']),\
                                                            'validacionDetected':str(x['validacionDetected'])})







    dirty | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_factDefiniciones_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_factDefiniciones,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    result = p.run()
    result.wait_until_finish()
