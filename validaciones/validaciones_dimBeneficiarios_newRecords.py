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
from apache_beam.runners.runner import PipelineState
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions





options1 = PipelineOptions(
    argv= None,
    runner='DataflowRunner',
    project='afiliados-pensionados-prote',
    job_name='dimbeneficiarios-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimBeneficiarios')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimBeneficiarios_dirty')


  
table_schema_dimBeneficiarios = {
    'fields': [{
        'name': 'BeneficiariosId', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'TipoIdentificacionBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IdentificacionBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IdentificacionAfiliado', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'fechaFinTemporalidad', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ConsecutivoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SecuenciaBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CalidadBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Sexo', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechasNacimiento', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SubtipoBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Parentesco', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Nombre', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimBeneficiarios_malos = {
    'fields': [{
        'name': 'BeneficiariosId', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'TipoIdentificacionBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IdentificacionBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IdentificacionAfiliado', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'fechaFinTemporalidad', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ConsecutivoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SecuenciaBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CalidadBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Sexo', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechasNacimiento', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SubtipoBeneficiario', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Parentesco', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Nombre', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}


def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

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
    if (element[key] is None  and element[key] == "None" and element[key] == "null"):
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


def fn_check_text(element,key):
    if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None"):
        if (str(element[key]).replace(" ","").isalpha() == False):
            element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no son texto,"
    return element


def fn_check_value_in_bene(element,key,listValues):
    if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None"):
        correct = False
        for value in listValues:
            if element[key] == value:
                correct = True
                break
        if correct == False:
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado en lista,"
    return element



def fn_check_son_younger_than_parent(element):
    if (element["Parentesco"] is not None  and element["Parentesco"] != "None" and element["Parentesco"] != "null") and \
       (element["FechasNacimiento"] is not None  and element["FechasNacimiento"] != "None" and element["FechasNacimiento"] != "null") and \
       (element["dtmfechanacimiento"] is not None  and element["dtmfechanacimiento"] != "None" and element["dtmfechanacimiento"] != "null"):
        if  "hijo" in str(element["Parentesco"]).lower():
            if str(element["dtmfechanacimiento"]).strip().replace("-","").isnumeric() and str(element["FechasNacimiento"]).strip().isnumeric():
                dtmfechanacimiento = float(str(element["dtmfechanacimiento"]).strip().replace("-",""))
                fechaNacimientoBeneficiario = float(str(element["FechasNacimiento"]).strip())        
                if dtmfechanacimiento >= fechaNacimientoBeneficiario:
                    element["validacionDetected"] = element["validacionDetected"] + "edad del  de beneficiario hijo es mayor o igual a la del afiliado,"
    return element




def fn_check_pension_survivor_older_25(element):
    if (element["Parentesco"] is not None  and element["Parentesco"] != "None" and element["Parentesco"] != "null") and \
       (element["FechasNacimiento"] is not None  and element["FechasNacimiento"] != "None" and element["FechasNacimiento"] != "null") and \
       (element["CalidadBeneficiario"] is not None  and element["CalidadBeneficiario"] != "None" and element["CalidadBeneficiario"] != "null") and \
       (element["TipoPension"] is not None  and element["TipoPension"] != "None" and element["TipoPension"] != "null"):
        if str(element["FechasNacimiento"]).strip().isnumeric():
            if len(str(element["FechasNacimiento"])) == 8:
                age = calculate_age(datetime.strptime(element["FechasNacimiento"], "%Y%m%d"))
                if element["TipoPension"] == "SOBREVIVENCIA":
                    if element["CalidadBeneficiario"] != "I":
                        if (float(age) >= float(25)):
                            element["validacionDetected"] = element["validacionDetected"] + "pension de sobrevivenvia a mayor de 25 años no invalido,"
                        elif  (float(age) < float(25) and age >= float(18)) and (element["Parentesco"].strip() != "ESTUDIANTES ENTRE 18 Y 25 AÑOS"):
                            element["validacionDetected"] = element["validacionDetected"] + "pension de sobrevivenvia a persona entre los 18 y 25 años que no es estudiante,"
    return element

    
def fn_check_age_son_ingesting(element):
    if (element["FechasNacimiento"] is not None  and element["FechasNacimiento"] != "None" and element["FechasNacimiento"] != "null") and \
       (element["Parentesco"] is not None  and element["Parentesco"] != "None" and element["Parentesco"] != "null") and \
       (element["CalidadBeneficiario"] is not None  and element["CalidadBeneficiario"] != "None" and element["CalidadBeneficiario"] != "null"):
        if str(element["FechasNacimiento"]).strip().isnumeric():
            if len(str(element["FechasNacimiento"])) == 8:
                age = calculate_age(datetime.strptime(element["FechasNacimiento"], "%Y%m%d"))
                if ("hijo" in element["Parentesco"].lower()) and ("estudiante" in element["Parentesco"].lower()):
                    if element["CalidadBeneficiario"] == "S":
                        if (age > float(25)):
                            element["validacionDetected"] = element["validacionDetected"] + "persona beneficiaria con parentesco hijo y calidad de beneficiario sano con mas de 25 años al momento del informe,"
    return element


def fn_check_age_parent_ingesting(element):
    if (element["FechasNacimiento"] is not None  and element["FechasNacimiento"] != "None" and element["FechasNacimiento"] != "null") and \
       (element["Parentesco"] is not None  and element["Parentesco"] != "None" and element["Parentesco"] != "null"):
        if str(element["FechasNacimiento"]).strip().isnumeric():
            if len(str(element["FechasNacimiento"])) == 8:
                age = calculate_age(datetime.strptime(element["FechasNacimiento"], "%Y%m%d"))
                if ("padre" in element["Parentesco"].lower()) and ("madre" in element["Parentesco"].lower()):
                    if (float(age) > float(95)):
                        element["validacionDetected"] = element["validacionDetected"] + "persona beneficiaria con parentesco madre o padre con mas de 95 años al momento del informe,"
    return element



def fn_check_bigger_than(element,key,valueToCompare):
    if (element[key] is not None  and element[key] == "None" and element[key] == "null"):
        if str(element[key]).isnumeric():
            try:
                correct = False
                cedulaNumerica = float(element[key].strip())
                if  cedulaNumerica < float(valueToCompare):
                    element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" es menor que "+str(valueToCompare)+","
            finally:
                pass
    return element




def fn_check_date_format(element,key,dateFormat):
    if (element[key] is not None  and element[key] != "None" and element[key] != "null"): 
        try:
            datetime.strptime(element[key], dateFormat)
        except:
            element["validacionDetected"] = element["validacionDetected"] + str(key) +" tiene un formato de fecha invalida," 
        finally:
            return element
    else:
        return element


def fn_check_word_lenght(element,key,compareTo):
    if (element[key] is not None  and element[key] != "None" and element[key] != "null"):
        if len(element[key]) < compareTo:
            element["validacionDetected"] = element["validacionDetected"] +element[key]+" tiene un longitud menor de "+str(compareTo)+","
    return element


if __name__ == "__main__":
    p = beam.Pipeline(options=options1)
    dimBeneficiarios  = (
        p
        | 'Query Table Beneficiarios' >> beam.io.ReadFromBigQuery(
            query='''
                SELECT GENERATE_UUID() as BeneficiariosId,TIPO_PENSION AS TipoPension, a.TIPO_ID_BENEFICIARIO as TipoIdentificacionBeneficiario, a.ID_BENEFICIARIO as IdentificacionBeneficiario,
                CURRENT_DATE() as FechaDato, a.idAfiliado as IdentificacionAfiliado, a.FECHA_FIN_TEMPORALIDAD as fechaFinTemporalidad,
                a.CONSECUTIVO_PENSION as ConsecutivoPension, a.BENS01 as SecuenciaBeneficiario, a.CALIDAD_BENEFICIARIO as CalidadBeneficiario,
                a.SEXO_BENEFICIARIO as Sexo, a.FECHA_NACIMIENTO_BENEFICIARIO as FechasNacimiento, /*'None' as IdentificacionPension, se habló con Maria y este campo se quita*/
                a.BENTI7 as TipoBeneficiario, a.BENSU2 as SubtipoBeneficiario, PARENTESCO as Parentesco, concat((SELECT CASE WHEN a.primerNombre IS null THEN " " ELSE a.primerNombre END AS primerNombre ), concat(" ",(SELECT CASE WHEN a.segundoNombre IS null THEN " " ELSE a.segundoNombre END AS segundoNombre )), concat(" ", (SELECT CASE WHEN a.primerApellido IS null THEN "" ELSE a.primerApellido END AS primerApellido )), concat(" ",(SELECT CASE WHEN a.segundoApellido IS null THEN " " ELSE a.segundoApellido END AS segundoApellido )) ) as Nombre, primerNombre,segundoNombre,primerApellido,segundoApellido, dtmfechanacimiento
                from afiliados-pensionados-prote.afiliados_pensionados.PenFutura a
                left join
                afiliados-pensionados-prote.Datamart.dimPensionados b
                on
                b.DocumentoDeLaPersona = a.idAfiliado
                left join
                afiliados-pensionados-prote.afiliados_pensionados.tabla_fed_Apolo f
                on
                f.idAfiliado=a.idAfiliado
                WHERE concat(a.idAfiliado,a.ID_BENEFICIARIO)
                NOT IN
                (SELECT concat( IdentificacionAfiliado , IdentificacionBeneficiario) as llave FROM `afiliados-pensionados-prote.Datamart.dimBeneficiarios`)
                ''',\
            use_standard_sql=True))








    dimBeneficiarios_Dict = dimBeneficiarios | beam.Map(lambda x: \
                                                            {'BeneficiariosId':str(x['BeneficiariosId']),\
                                                             'TipoIdentificacionBeneficiario':str(x['TipoIdentificacionBeneficiario']),\
                                                             'IdentificacionBeneficiario':str(x['IdentificacionBeneficiario']),\
                                                             'FechaDato':str(x['FechaDato']),\
                                                             'IdentificacionAfiliado':str(x['IdentificacionAfiliado']),\
                                                             'fechaFinTemporalidad':str(x['fechaFinTemporalidad']),\
                                                             'ConsecutivoPension':str(x['ConsecutivoPension']),\
                                                             'SecuenciaBeneficiario':str(x['SecuenciaBeneficiario']),\
                                                             'CalidadBeneficiario':str(x['CalidadBeneficiario']),\
                                                             'Sexo':str(x['Sexo']),\
                                                             'FechasNacimiento':str(x['FechasNacimiento']),\
                                                             'TipoBeneficiario':str(x['TipoBeneficiario']),\
                                                             'SubtipoBeneficiario':str(x['SubtipoBeneficiario']),\
                                                             'Parentesco':str(x['Parentesco']),\
                                                             'TipoPension':str(x['TipoPension']),\
                                                             'Nombre':str(x['Nombre']),\
                                                             'primerNombre':str(x['primerNombre']),\
                                                             'segundoNombre':str(x['segundoNombre']),\
                                                             'primerApellido':str(x['primerApellido']),\
                                                             'segundoApellido':str(x['segundoApellido']),\
                                                             'dtmfechanacimiento':str(x['dtmfechanacimiento']),\
                                                             'validacionDetected':""})



    TipoIdentificacionBeneficiario_fullness_validated = dimBeneficiarios_Dict | 'completitud TipoIdentificacionBeneficiario' >> beam.Map(fn_check_completitud,    'TipoIdentificacionBeneficiario' )

    TipoIdentificacionBeneficiario_text_validated = TipoIdentificacionBeneficiario_fullness_validated | 'solo texto TipoIdentificacionBeneficiario' >> beam.Map( fn_check_text,  'TipoIdentificacionBeneficiario')

    FechasNacimiento_fullness_validated = TipoIdentificacionBeneficiario_text_validated | 'completitud FechasNacimiento' >> beam.Map(fn_check_completitud,    'FechasNacimiento' )

    FechasNacimiento_dateFormat_validated = FechasNacimiento_fullness_validated | 'date format FechasNacimiento' >> beam.Map(fn_check_date_format,    'FechasNacimiento', "%Y%m%d" )

    IdentificacionBeneficiario_fullness_validated = FechasNacimiento_dateFormat_validated | 'completitud IdentificacionBeneficiario' >> beam.Map(fn_check_completitud,    'IdentificacionBeneficiario' )

    IdentificacionBeneficiario_numbers_validated = IdentificacionBeneficiario_fullness_validated | 'solo numeros IdentificacionBeneficiario' >> beam.Map( fn_check_numbers,  'IdentificacionBeneficiario')

    IdentificacionBeneficiario_biggerThan_validated = IdentificacionBeneficiario_numbers_validated | 'bigger than IdentificacionBeneficiario' >> beam.Map(fn_check_bigger_than,    'IdentificacionBeneficiario',0 )

    fechaFinTemporalidad_dateFormat_validated = IdentificacionBeneficiario_biggerThan_validated | 'date format fechaFinTemporalidad' >> beam.Map(fn_check_date_format,    'fechaFinTemporalidad', "%Y%m%d" )

    fechaFinTemporalidad_biggerThan_validated = fechaFinTemporalidad_dateFormat_validated | 'bigger than fechaFinTemporalidad' >> beam.Map(fn_check_bigger_than,    'fechaFinTemporalidad', 20131231)

    ConsecutivoPension_fullness_validated = fechaFinTemporalidad_biggerThan_validated | 'completitud ConsecutivoPension' >> beam.Map(fn_check_completitud,    'ConsecutivoPension' )

    ConsecutivoPension_numbers_validated = ConsecutivoPension_fullness_validated | 'solo numeros ConsecutivoPension' >> beam.Map( fn_check_numbers,  'ConsecutivoPension')

    SecuenciaBeneficiario_fullness_validated = ConsecutivoPension_numbers_validated | 'completitud SecuenciaBeneficiario' >> beam.Map(fn_check_completitud,    'SecuenciaBeneficiario' )

    SecuenciaBeneficiario_numbers_validated = SecuenciaBeneficiario_fullness_validated | 'solo numeros SecuenciaBeneficiario' >> beam.Map( fn_check_numbers,  'SecuenciaBeneficiario')

    CalidadBeneficiario_fullness_validated = SecuenciaBeneficiario_numbers_validated | 'completitud CalidadBeneficiario' >> beam.Map(fn_check_completitud,    'CalidadBeneficiario' )

    Sexo_fullness_validated = CalidadBeneficiario_fullness_validated | 'completitud Sexo' >> beam.Map(fn_check_completitud,    'Sexo' )

    Sexo_text_validated = Sexo_fullness_validated | 'solo texto sexo' >> beam.Map( fn_check_text, 'Sexo')

    Sexo_MoF_validated = Sexo_text_validated | 'sexo en valores' >> beam.Map( fn_check_value_in_bene,'Sexo',["M","F"])

    TipoBeneficiario_fullness_validated = Sexo_MoF_validated | 'completitud TipoBeneficiario' >> beam.Map(fn_check_completitud,    'TipoBeneficiario' )

    SubtipoBeneficiario_fullness_validated = TipoBeneficiario_fullness_validated | 'completitud SubtipoBeneficiario' >> beam.Map(fn_check_completitud,    'SubtipoBeneficiario' )

    Parentesco_fullness_validated = SubtipoBeneficiario_fullness_validated | 'completitud Parentesco' >> beam.Map(fn_check_completitud,    'Parentesco' )

    Son_youngerThan_parents = Parentesco_fullness_validated | 'son younger than parents' >> beam.Map(fn_check_son_younger_than_parent)

    TipoPension_QualityRule_6_checked = Son_youngerThan_parents | 'there can not be survivor money for helthy people older than 25' >> beam.Map(fn_check_pension_survivor_older_25)

    FechasNacimiento_QualityRule_25 = TipoPension_QualityRule_6_checked | 'there can not be son with healthy quality older than 25' >> beam.Map(fn_check_age_son_ingesting)

    FechasNacimiento_QualityRule_26 = FechasNacimiento_QualityRule_25 | 'there can not be parents older than 95' >> beam.Map(fn_check_age_parent_ingesting)

    BeneficiariosId_fullness_validated = FechasNacimiento_QualityRule_26 | 'completitud BeneficiariosId' >> beam.Map(fn_check_completitud,    'BeneficiariosId' )

    FechaDato_fullness_validated = BeneficiariosId_fullness_validated | 'completitud FechaDato' >> beam.Map(fn_check_completitud,    'FechaDato' )

    PrimerNombre_fullness_validated = FechaDato_fullness_validated | 'completitud primerNombre' >> beam.Map(fn_check_completitud,    'primerNombre' )

    primerApellido_fullness_validated = PrimerNombre_fullness_validated | 'completitud primerApellido' >> beam.Map(fn_check_completitud,    'primerApellido' )

    Nombre_text_validated = primerApellido_fullness_validated | 'solo texto Nombre' >> beam.Map( fn_check_text,  'Nombre')

    PrimerNombre_QualityRule_31 = Nombre_text_validated  | 'solo texto primerNombre' >> beam.Map( fn_check_word_lenght,  'primerNombre', 2)

    PrimerApellido_QualityRule_31 = PrimerNombre_QualityRule_31  | 'solo texto primerApellido' >> beam.Map( fn_check_word_lenght,  'primerApellido', 2)





    results = PrimerApellido_QualityRule_31 | beam.ParDo(fn_divide_clean_dirty()).with_outputs()

    limpias = results["Clean"] | beam.Map(lambda x:   {'BeneficiariosId':str(x['BeneficiariosId']),\
                                                       'TipoIdentificacionBeneficiario':str(x['TipoIdentificacionBeneficiario']),\
                                                       'IdentificacionBeneficiario':str(x['IdentificacionBeneficiario']),\
                                                       'FechaDato':str(x['FechaDato']),\
                                                       'IdentificacionAfiliado':str(x['IdentificacionAfiliado']),\
                                                       'fechaFinTemporalidad':str(x['fechaFinTemporalidad']),\
                                                       'ConsecutivoPension':str(x['ConsecutivoPension']),\
                                                       'SecuenciaBeneficiario':str(x['SecuenciaBeneficiario']),\
                                                       'CalidadBeneficiario':str(x['CalidadBeneficiario']),\
                                                       'Sexo':str(x['Sexo']),\
                                                       'FechasNacimiento':str(x['FechasNacimiento']),\
                                                       'TipoBeneficiario':str(x['TipoBeneficiario']),\
                                                       'SubtipoBeneficiario':str(x['SubtipoBeneficiario']),\
                                                       'Nombre':str(x['Nombre']),\
                                                       'Parentesco':str(x['Parentesco'])})




    dirty = results["validationsDetected"] | beam.Map(lambda x:   {'BeneficiariosId':str(x['BeneficiariosId']),\
                                                       'TipoIdentificacionBeneficiario':str(x['TipoIdentificacionBeneficiario']),\
                                                       'IdentificacionBeneficiario':str(x['IdentificacionBeneficiario']),\
                                                       'FechaDato':str(x['FechaDato']),\
                                                       'IdentificacionAfiliado':str(x['IdentificacionAfiliado']),\
                                                       'fechaFinTemporalidad':str(x['fechaFinTemporalidad']),\
                                                       'ConsecutivoPension':str(x['ConsecutivoPension']),\
                                                       'SecuenciaBeneficiario':str(x['SecuenciaBeneficiario']),\
                                                       'CalidadBeneficiario':str(x['CalidadBeneficiario']),\
                                                       'Sexo':str(x['Sexo']),\
                                                       'FechasNacimiento':str(x['FechasNacimiento']),\
                                                       'TipoBeneficiario':str(x['TipoBeneficiario']),\
                                                       'SubtipoBeneficiario':str(x['SubtipoBeneficiario']),\
                                                       'Nombre':str(x['Nombre']),\
                                                       'Parentesco':str(x['Parentesco']),\
                                                       'validacionDetected':str(x['validacionDetected'])})




    dirty | "write dirty ones" >> beam.io.WriteToBigQuery(table_spec_dirty,
                                                            schema=table_schema_dimBeneficiarios_malos,
                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(table_spec_clean,
                                                            schema=table_schema_dimBeneficiarios,
                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    result = p.run()
    result.wait_until_finish()
