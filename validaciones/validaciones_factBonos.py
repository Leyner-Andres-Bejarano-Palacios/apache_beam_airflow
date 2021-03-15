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
    job_name='factbonos-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='factBonos')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='factBonos_dirty')


  
table_schema_factBonos = {
    'fields': [
        {
        'name':'InfPersonasId', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Bono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Tasa', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaRedencionBono', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'FaltanteBonoPensional', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoBonoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EntidadEmisoraBonoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'VersionBono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CalculosActuarialesID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CuponesBonoID', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_factBonos_malos = {
    'fields': [{
        'name':'InfPersonasId', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Bono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Tasa', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaRedenciónBono', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'FaltanteBonoPensional', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoBonoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EntidadEmisoraBonoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'VersionBono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CalculosActuarialesID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CuponesBonoID', 'type':'STRING', 'mode':'NULLABLE'},
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
        if float(element[key]) < float(valueToCompare):
            element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" es menor que "+str(valueToCompare)+","
    return element

def fn_check_text(element,key):
    if (element[key] is not None) and (str(element[key]) != "null") and (element[key] != "None"):
        if (str(element[key]).replace(" ","").isalpha() == False):
            element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" no son texto,"
    return element

def fn_check_pension_less_than_50(element):
    if element["TipoPension"].trim() == "2" or element["TipoPension"].trim() == "3":
        if float(element["SemanasMomentoDefinicion"]) < float(50):
            element["validacionDetected"] = element["validacionDetected"] + "persona con pension de invalidez o sobrevivencia con menos de 50 en campo total_semanas - semanasAlMomentoDeLaDefinicion,"
    return element


if __name__ == "__main__":
    p = beam.Pipeline(options=options1)
    factBonos  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                SELECT GENERATE_UUID() as BonoId, B.InfPersonasId, CURRENT_DATE() as FechaDato, fu.BONO Bono, C.FR_BONO FechaRedencionBono, C.MODALIDAD,
                SP.TASA Tasa, 'None' FaltanteBonoPensional, EstadoBonoID, EntidadEmisoraBonoID , SC.VERSION_CUPON VersionBono,
                'None' CalculosActuarialesID, ZZZ.CuponesBonoID, Atributo
                FROM
                afiliados-pensionados-prote.afiliados_pensionados.FuturaPen C
                LEFT JOIN
                afiliados-pensionados-prote.Datamart.dimPensionados PEN
                ON
                C.idAfiliado=PEN.DocumentoDeLaPersona
                LEFT JOIN
                afiliados-pensionados-prote.Datamart.dimBeneficiarios BEN
                ON
                BEN.IdentificacionAfiliado=PEN.DocumentoDeLaPersona
                LEFT JOIN
                afiliados-pensionados-prote.Datamart.dimInfPersonas B
                ON
                B.PensionadosId=PEN.PensionadosID AND B.BeneficiariosId=BEN.BeneficiariosID
                LEFT JOIN
                (SELECT BONO, idAfiliado, 'Pensionados' as Atributo FROM afiliados-pensionados-prote.afiliados_pensionados.FuturaPen
                UNION ALL
                (SELECT BONO, idAfiliado, 'Retiros' as Atributo FROM afiliados-pensionados-prote.afiliados_pensionados.tabla_fed_retiros_futura a
                LEFT JOIN
                notional-radio-302217.DatalakeAnalitica.SQLSERVER_BIPROTECCIONDW_DW_DIMAFILIADOS_Z51 b
                on
                a.afi_hash64=b.afi_hash64)
                )fu
                ON
                fu.idAfiliado=C.idAfiliado
                LEFT JOIN
                afiliados-pensionados-prote.afiliados_pensionados.SUPERCUPON SC
                on
                SC.IDENTIFICACION=C.idAfiliado
                LEFT JOIN
                afiliados-pensionados-prote.Datamart.dimEstadoCupon ft
                ON
                ft.EstadoCupon=SC.ESTADO_CUPON
                LEFT JOIN
                afiliados-pensionados-prote.Datamart.dimCuponBono ZZZ
                ON
                ZZZ.EstadoCuponID=ft.EstadoCuponID
                LEFT JOIN
                (
                SELECT TASA,idAfiliado,idEmpleador,bones4 FROM
                afiliados-pensionados-prote.afiliados_pensionados.SUPERBONO2
                
                ) SP
                ON
                SP.idAfiliado=C.idAfiliado
                left join
                afiliados-pensionados-prote.Datamart.dimEntidadEmisorBono gg
                on
                gg.IdEntidadEmisora=SP.idEmpleador
                left join
                afiliados-pensionados-prote.Datamart.dimEstadoBono ggp
                on
                ggp.EstadoBono=SP.bones4
                ''',\
            use_standard_sql=True))


    factBonos_Dict = factBonos | beam.Map(lambda x: {'InfPersonasId':str(x['InfPersonasId']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'Bono':str(x['Bono']),\
                                                            'Tasa':str(x['Tasa']),\
                                                            'FechaRedencionBono':str(x['FechaRedencionBono']),\
                                                            'FaltanteBonoPensional':str(x['FaltanteBonoPensional']),\
                                                            'EstadoBonoID':str(x['EstadoBonoID']),\
                                                            'EntidadEmisoraBonoID':str(x['EntidadEmisoraBonoID']),\
                                                            'VersionBono':str(x['VersionBono']),\
                                                            'CalculosActuarialesID':str(x['CalculosActuarialesID']),\
                                                            'CuponesBonoID':str(x['CuponesBonoID']),\
                                                            'validacionDetected':""})


    InfPersonasId_fullness_validated = factBonos_Dict | 'completitud InfPersonasId' >> beam.Map(fn_check_completitud,    'InfPersonasId' )

    FechaDato_fullness_validated = InfPersonasId_fullness_validated | 'completitud FechaDato' >> beam.Map(fn_check_completitud,    'FechaDato' )

    Tasa_fullness_validated = FechaDato_fullness_validated | 'completitud Tasa' >> beam.Map(fn_check_completitud,    'Tasa' )

    FechaRedencionBono_fullness_validated = Tasa_fullness_validated | 'completitud FechaRedencionBono' >> beam.Map(fn_check_completitud,    'FechaRedencionBono' )

    FaltanteBonoPensional_fullness_validated = FechaRedencionBono_fullness_validated | 'completitud FaltanteBonoPensional' >> beam.Map(fn_check_completitud,    'FaltanteBonoPensional' )

    EstadoBonoID_fullness_validated = FaltanteBonoPensional_fullness_validated | 'completitud EstadoBonoID' >> beam.Map(fn_check_completitud,    'EstadoBonoID' )

    EntidadEmisoraBonoID_fullness_validated = EstadoBonoID_fullness_validated | 'completitud EntidadEmisoraBonoID' >> beam.Map(fn_check_completitud,    'EntidadEmisoraBonoID' )

    VersionBono_fullness_validated = EntidadEmisoraBonoID_fullness_validated | 'completitud VersionBono' >> beam.Map(fn_check_completitud,    'VersionBono' )

    CalculosActuarialesID_fullness_validated = VersionBono_fullness_validated | 'completitud CalculosActuarialesID' >> beam.Map(fn_check_completitud,    'CalculosActuarialesID' )

    CuponesBonoID_fullness_validated = CalculosActuarialesID_fullness_validated | 'completitud CuponesBonoID' >> beam.Map(fn_check_completitud,    'CuponesBonoID' )



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

    results = CuponesBonoID_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x:         {'InfPersonasID':str(x['InfPersonasID']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'Bono':str(x['Bono']),\
                                                            'Tasa':str(x['Tasa']),\
                                                            'FechaRedencionBono':str(x['FechaRedencionBono']),\
                                                            'FaltanteBonoPensional':str(x['FaltanteBonoPensional']),\
                                                            'EstadoBonoID':str(x['EstadoBonoID']),\
                                                            'EntidadEmisoraBonoID':str(x['EntidadEmisoraBonoID']),\
                                                            'VersionBono':str(x['FechaRedenciVersionBonoonBono']),\
                                                            'CalculosActuarialesID':str(x['CalculosActuarialesID']),\
                                                            'CuponesBonoID':str(x['CuponesBonoID'])})


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
            schema=table_schema_factBonos_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_factBonos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            
    result = p.run()
    result.wait_until_finish()
