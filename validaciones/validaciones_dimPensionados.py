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
    job_name='dimpensionados-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimPensionados')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimPensionados_dirty')


  
table_schema_dimPensionados = {
    'fields': [{
        'name': 'FechaDelDato', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'PensionadosID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ModalidadPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IndicadorPensionGPM', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'NumeroSolicitudPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'tipoDePension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ModalidadInicial', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DocumentoDeLaPersona', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoDocumentoDeLaPersona', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoDePersona', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaSiniestro', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaNacimiento', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Sexo', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Nombre', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Mesadas', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_dimPensionados_malos = {
    'fields': [{
        'name': 'FechaDelDato', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'PensionadosID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ModalidadPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IndicadorPensionGPM', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'NumeroSolicitudPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'tipoDePension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'ModalidadInicial', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DocumentoDeLaPersona', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoDocumentoDeLaPersona', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoDePersona', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaPension', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaNacimiento', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaSiniestro', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Sexo', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Nombre', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Mesadas', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

def calculate_age_sinister_date(born,fechaSinister):
    return fechaSinister.year - born.year - ((fechaSinister.month, fechaSinister.day) < (born.month, born.day))

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


def fn_check_completitud_bene(key):
    if (element[key] is None  and element[key] == "None" and element[key] == "null") and \
        element["TipoPersona"] == "Beneficiario":
        element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
    return element


def fn_check_value_in(element,key,listValues):
    if (element[key] is not None  and element[key] != "None" and element[key] != "null"):
        if  element[key] not in listValues:
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado en lista,"
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





def fn_bene_younger_cliient(element):
    if (element["ParentescoBeneficiario"] is None  and element["ParentescoBeneficiario"] == "None" and element["ParentescoBeneficiario"] == "null") and \
       (element["FechaNacimiento"] is None  and element["FechaNacimiento"] == "None" and element["FechaNacimiento"] == "null") and \
       (element["fechaNacimientoAfiliado"] is None  and element["fechaNacimientoAfiliado"] == "None" and element["fechaNacimientoAfiliado"] == "null"):
       if element["ParentescoBeneficiario"] == "Hijo mayor de 25 años" and  \
          element["ParentescoBeneficiario"] == "HIJOS MENORES DE 18 AÑOS" and  \
          element["ParentescoBeneficiario"] == "HIJO":
            fechaNacimiento = datetime.strptime( element["FechaNacimiento"], "%Y-%m-%d")
            today = datetime.now()
            time_difference =  today - fechaNacimiento
            age = time_difference.days
            fechaNacimientoAfiliado = datetime.strptime( element["FechaNacimiento"], "%Y-%m-%d")
            today = datetime.now()
            time_difference =  today - fechaNacimientoAfiliado
            ageAfiliado = time_difference.days
            if age >= ageAfiliado:
                element["validacionDetected"] = element["validacionDetected"] + "edad de beneficiario hijo mayor a la del afiliado,"
    return element



def fn_bene_older_cliient(element):
    if (element["ParentescoBeneficiario"] is None  and element["ParentescoBeneficiario"] == "None" and element["ParentescoBeneficiario"] == "null") and \
       (element["FechaNacimiento"] is None  and element["FechaNacimiento"] == "None" and element["FechaNacimiento"] == "null") and \
       (element["fechaNacimientoAfiliado"] is None  and element["fechaNacimientoAfiliado"] == "None" and element["fechaNacimientoAfiliado"] == "null"):
       if element["ParentescoBeneficiario"] == "PADRE" and  \
          element["ParentescoBeneficiario"] == "MADRE" and  \
          element["ParentescoBeneficiario"] == "ABUELO":
            fechaNacimiento = datetime.strptime( element["FechaNacimiento"], "%Y-%m-%d")
            today = datetime.now()
            time_difference =  today - fechaNacimiento
            age = time_difference.days
            fechaNacimientoAfiliado = datetime.strptime( element["FechaNacimiento"], "%Y-%m-%d")
            today = datetime.now()
            time_difference =  today - fechaNacimientoAfiliado
            ageAfiliado = time_difference.days
            if  ageAfiliado >=   age:
                element["validacionDetected"] = element["validacionDetected"] + "edad de beneficiario madre, padre o abuelo menor a la del afiliado,"
    return element

def fn_repetead_id(key,number):
    if (element[key] is None  and element[key] == "None" and element[key] == "null"):
        if int(element[key]) > number:
            element["validacionDetected"] = element["validacionDetected"] + "hay dos o mas usuarios con la misma cedula,"
    return element

def fn_bene_short_name(key):
   if (element[key] is None  and element[key] == "None" and element[key] == "null"):
    names = element[key].split()
    for name in names:
        if len(name) < 2:
            element["validacionDetected"] = element["validacionDetected"] + "uno de los nombres o apellidos es tiene una longitud menor a 2 caracteres,"
            break
    return element

def fn_fimd_no_mathcing(key1,key2):
    if  (element[key1] is None  and element[key1] == "None" and element[key1] == "null") and \
        (element[key2] is None  and element[key2] == "None" and element[key2] == "null") :
        if element[key1] != element[key2]:
            element["validacionDetected"] = element["validacionDetected"] + "informacion en AFIES2 de la tabla AS400_FPOBLIDA_AFIARC no coincide con la informacion en idEstadoAfiliado de la tabla apolo en el DW,"


def fn_check_pension_oldeness_women_men(element):
    if (element["tipoDePension"] is not None  and element["tipoDePension"] != "None" and element["tipoDePension"] != "null") and \
       (element["Sexo"] is not None  and element["Sexo"] != "None" and element["Sexo"] != "null") and \
       (element["VejezAnticipada"] is not None  and element["VejezAnticipada"] != "None" and element["VejezAnticipada"] != "null") and \
       (element["FechaNacimiento"] is not None  and element["FechaNacimiento"] != "None" and element["FechaNacimiento"] != "null"):
        if str(element["FechaNacimiento"]).strip().replace("-","").isnumeric():
            if len(str(element["FechaNacimiento"]).strip().replace("-","")) == 8:
                if element["tipoDePension"] == "1":
                    age = calculate_age(datetime.strptime(str(element["FechaNacimiento"]).strip().replace("-",""), "%Y%m%d"))
                    if (element["Sexo"] == "FEMENINO") and (float(age) < float(57) and (element["VejezAnticipada"] != "VEJEZ ANTICIPADA")):
                        element["validacionDetected"] = element["validacionDetected"] + "mujer con pension de vejez menor a 57 años de edad sin vejez anticipada,"
                    elif (element["Sexo"] != "MASCULINO") and (float(age) < float(62) and (element["VejezAnticipada"] != "VEJEZ ANTICIPADA" )):
                        element["validacionDetected"] = element["validacionDetected"] + "mhombre  con pension de vejez menor a 62 años de edad sin vejez anticipada,"
    return element




def fn_check_pension_tipoPension_2(element):
    if (element["tipoDePension"] is not None  and element["tipoDePension"] != "None" and element["tipoDePension"] != "null") and \
       (element["Sexo"] is not None  and element["Sexo"] != "None" and element["Sexo"] != "null") and \
       (element["FechaNacimiento"] is not None  and element["FechaNacimiento"] != "None" and element["FechaNacimiento"] != "null"):
        if str(element["FechaNacimiento"]).strip().replace("-","").isnumeric():
            if len(str(element["FechaNacimiento"]).strip().replace("-","")) == 8:
                if element["tipoDePension"] == "2":
                    age = calculate_age(datetime.strptime(str(element["FechaNacimiento"]).strip().replace("-",""), "%Y%m%d"))
                    if (element["Sexo"] == "FEMENINO") and (float(age) < float(57)):
                        element["validacionDetected"] = element["validacionDetected"] + "mujer con pension de vejez menor a 57 años de edad,"
                    elif (element["Sexo"] != "MASCULINO") and (float(age) < float(62)):
                        element["validacionDetected"] = element["validacionDetected"] + "hombre  con pension de vejez menor a 62 años de edad,"
    return element



def fn_check_pension_oldeness(element):
    if (element["tipoDePension"] is not None  and element["tipoDePension"] != "None" and element["tipoDePension"] != "null") and \
       (element["FechaNacimiento"] is not None  and element["FechaNacimiento"] != "None" and element["FechaNacimiento"] != "null"):
       if str(element["FechaNacimiento"]).strip().replace("-","").isnumeric():
           if len(str(element["FechaNacimiento"]).strip().replace("-","")) == 8:
               if element["tipoDePension"] == "1":
                   age = calculate_age(datetime.strptime(str(element["FechaNacimiento"]).strip().replace("-",""), "%Y%m%d"))
                   if (float(age) > float(95)):
                       element["validacionDetected"] = element["validacionDetected"] + "pensionado conm edad superior a 95 años,"
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


def fn_check_date_compare(element,key1,dateFormat1,key2,dateFormat2):
    if (element[key] is not None  and element[key] != "None" and element[key] != "null"): 
        try:
            fechasiniestro  = datetime.strptime(element[key1], dateFormat1)
            fechaNacimiento = datetime.strptime(element[key2], dateFormat2)
            if fechaNacimiento > fechasiniestro:
                element["validacionDetected"] = element["validacionDetected"] + " fecha de siniestro mayor a fecha de nacimiento,"     
        except:
            element["validacionDetected"] = element["validacionDetected"] + str(key1) +" o "+str(key2)+""+" tiene un formato de fecha invalida," 
        finally:
            return element
    else:
        return element


def fn_check_bigger_than(element,key,valueToCompare):
    if (element[key] is not None  and element[key] != "None" and element[key] != "null"):
        try:
            correct = False
            cedulaNumerica = float(element[key].strip())
            if  cedulaNumerica < float(valueToCompare):
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key) +" es menor que "+str(valueToCompare)+","
        finally:
            pass
    return element


def fn_rentavitalicia_pencon_24(element):
    if (element["ModalidadPension"] is not None  and element["ModalidadPension"] != "None" and element["ModalidadPension"] != "ModalidadPension") and \
       (element["PENCON"] is not None  and element["PENCON"] != "None" and element["PENCON"] != "PENCON"):
       if str(element["PENCON"]).isnumeric() and str(element["ModalidadPension"]).isnumeric():
            if int(float(str(element["ModalidadPension"]))) == 2:
                if int(float(str(element["PENCON"]))) == 24:
                    pass
            else:
                element["validacionDetected"] = element["validacionDetected"] + "modalidad de pension 2, es decir rentavitalicia con PENCON de la PENARC diferente a 24,"
       else:
            element["validacionDetected"] = element["validacionDetected"] + "no se puede validar si rentavitalicia tiene PENCON de la PENARC igual a 24 ya que uno de estos no es numerivo,"
    return element

def fn_gpm_less_than_1150(element):
    if (element["IndicadorPensionGPM"] is not None  and \
        element["IndicadorPensionGPM"] != "None" and \
        element["IndicadorPensionGPM"] != "null" and \
        element["TotalSemanas"] is not None and \
        element["TotalSemanas"] != "None" and \
        str(element["TotalSemanas"]).replace(" ","").isnumeric() and \ 
        element["TotalSemanas"] != "null"):
        if str(element["IndicadorPensionGPM"]).replace(" ","") == "s" and \
           float(str(element["TotalSemanas"]).replace(" ","")) < float(1150):
            element["validacionDetected"] = element["validacionDetected"] + "Pensiones de Garantia de Pension Minima con menos de 1150 semanas,"
    return element


def fn_age_to_fechaSiniestro(element):
    if (element["IndicadorPensionGPM"] is not None  and \
        element["IndicadorPensionGPM"] != "None" and \
        element["IndicadorPensionGPM"] != "null" and \
        element["FechaSiniestro"] is not None  and \
        element["FechaSiniestro"] != "None" and \
        element["FechaSiniestro"] != "null" and \
        element["Sexo"] is not None  and \
        element["Sexo"] != "None" and \
        element["Sexo"] != "null" and \
        element["FechaNacimiento"] is not None  and \
        element["FechaNacimiento"] != "None" and \
        element["FechaNacimiento"] != "null"):
        try:
            fechaNacimiento   = datetime.strptime(element["FechaSiniestro"], "%Y%m%d")
            fechaSiniestro    = datetime.strptime(element["FechaNacimiento"], "%Y-%m-%d")
            ageToSinisterDate = calculate_age_sinister_date(fechaNacimiento,fechaSiniestro)
            if str(element["IndicadorPensionGPM"]).replace(" ","") == "s":
                if   (element["Sexo"] == "FEMENINO") and (float(ageToSinisterDate) < float(57)):
                    element["validacionDetected"] = element["validacionDetected"] + "edad a la fecha de siniestro es inferior a la debida para mujeres  en casos GPM,"                    
                elif (element["Sexo"] == "MASCULINO") and (float(ageToSinisterDate) < float(62)):
                    element["validacionDetected"] = element["validacionDetected"] + "edad a la fecha de siniestro es inferior a la debida para hombres en casos GPM,"
        except:
            pass
    return element


def fn_repeatedId_different_docType(element):
    if (element["prueba"] is not None  and\
        element["prueba"] != "None" and\
        element["prueba"] != "null") and\
        str(element["prueba"]).strip().isnumeric() :
        if float(str(element["prueba"]).strip()) > float(1):
            element["validacionDetected"] = element["validacionDetected"] + " documento repetido con diferentes tipos de documentacion ,"
    return element

def fn_gcpm_case_different_program_retire(element):
    if (element["ModalidadPension"] is not None  and\
        element["ModalidadPension"] != "None" and\
        element["ModalidadPension"] != "null") and\
        element["ModalidadPension"].strip().isnumeric() and\
        element["IndicadorPensionGPM"] is not None  and \
        element["IndicadorPensionGPM"] != "None" and \
        element["IndicadorPensionGPM"] != "null":
        if str(element["IndicadorPensionGPM"]).replace(" ","") == "s":
            if float(str(element["ModalidadPension"]).strip()) not in [float(1),float(2),float(6)]:
                element["validacionDetected"] = element["validacionDetected"] + "caso GCP con pension diferente a retiro programado,"
    return element


def fn_total_weeks_afiliafos_devoluciones(element):
    if (element["TOTAL_SEMANAS"] is not None  and\
        element["TOTAL_SEMANAS"] != "None" and\
        element["TOTAL_SEMANAS"] != "null") and\
        element["TOTAL_SEMANAS"].strip().isnumeric() and\
        element["afiliadosDevoluciones"] is not None  and \
        element["afiliadosDevoluciones"] != "None" and \
        element["afiliadosDevoluciones"] != "null":
        if float(str(element["TOTAL_SEMANAS"]).strip()) == float(0):
            element["validacionDetected"] = element["validacionDetected"] + "afiliado con Devoluciones de Saldo con semanas en cero,"
    return element


def fn_total_weeks_afiliafos_devoluciones_vejez(element):
    if (element["TOTAL_SEMANAS"] is not None  and\
        element["TOTAL_SEMANAS"] != "None" and\
        element["TOTAL_SEMANAS"] != "null") and\
        element["TOTAL_SEMANAS"].strip().isnumeric() and\
        element["afiliadosDevoluciones"] is not None  and \
        element["afiliadosDevoluciones"] != "None" and \
        element["afiliadosDevoluciones"] != "null" and \
        element["tipoDePension"] is not None  and \
        element["tipoDePension"] != "None" and \
        element["tipoDePension"] != "null" and \
        if element["tipoDePension"] == "2":
            if float(str(element["TOTAL_SEMANAS"]).strip()) > float(1150):
                element["validacionDetected"] = element["validacionDetected"] + "riesgo vejez y con un numero de semanas superior a 1.150,"        
    return element


if __name__ == "__main__":
    p = beam.Pipeline(options=options1)
    dimPensionados  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                    select PensionadosID, DocumentoDeLaPersona,TipoDocumentoDeLaPersona, FechaDelDato,
                    ModalidadPension, IndicadorPensionGPM, NumeroSolicitudPension, tipoDePension,
                    EstadoPension, ModalidadInicial, FechaPension, FechaSiniestro, Sexo, FechaNacimiento, Nombre, Mesadas, Procesos,PENCON,VejezAnticipada, aparicionesID,
                    vg.prueba,aparicionesModalidad, ff.pruebamodalidad
                    from(select GENERATE_UUID() as PensionadosID, DocumentoDeLaPersona,TipoDocumentoDeLaPersona, FechaDelDato,
                    ModalidadPension, IndicadorPensionGPM, NumeroSolicitudPension, tipoDePension,
                    EstadoPension, ModalidadInicial, FechaPension, FechaSiniestro, Sexo, FechaNacimiento, Nombre, Mesadas, Procesos,f.PENCON,VejezAnticipada,
                    case when count(DocumentoDeLaPersona)>1 then DocumentoDeLaPersona end as aparicionesID,
                    from (SELECT distinct a.idAfiliado as DocumentoDeLaPersona, a.tipoIdAfiliado as TipoDocumentoDeLaPersona, CURRENT_DATE() as FechaDelDato,
                    a.MODALIDAD as ModalidadPension,
                    a.INDICADOR_GPM as IndicadorPensionGPM, a.NUMERO_SOLICITUD as NumeroSolicitudPension,
                    a.TIPO_PENSION as tipoDePension, a.ESTADO_PENSION as EstadoPension,
                    a.MODALIDAD as ModalidadInicial,
                    a.FECHA_PENSION FechaPension, a.FECHA_SINIESTRO FechaSiniestro,
                    b.descSexo as Sexo, b.dtmFechaNacimiento as FechaNacimiento, b.nombreCompletoAfiliado Nombre, a.NUMERO_MESADAS Mesadas, A.PROCESO Procesos,
                    case when(a.SOLBP_CODCAUMOD='@003' and a.TIPO_SOLICITUD='VEJ') THEN 'VEJEZ ANTICIPADA' else 'None' END AS VejezAnticipada
                    FROM
                    afiliados-pensionados-prote.afiliados_pensionados.PenFutura a

                    left join
                    afiliados-pensionados-prote.afiliados_pensionados.tabla_fed_Apolo b
                    on
                    a.idAfiliado=b.idAfiliado)
                    LEFT JOIN
                    notional-radio-302217.DatalakeAnalitica.SQLSERVER_BIPROTECCIONDW_DW_DIMAFILIADOS_Z51 j
                    ON
                    j.idAfiliado=DocumentoDeLaPersona
                    left join
                    notional-radio-302217.DatalakeAnalitica.AS400_FPOBLIDA_PENARC f
                    on
                    f.afi_hash64=j.afi_hash64
                    GROUP by DocumentoDeLaPersona,
                    TipoDocumentoDeLaPersona, FechaDelDato,
                    ModalidadPension, IndicadorPensionGPM, NumeroSolicitudPension, tipoDePension,
                    EstadoPension, ModalidadInicial, FechaPension, FechaSiniestro, Sexo, FechaNacimiento, Nombre, Mesadas, Procesos,f.PENCON,VejezAnticipada
                    having count(DocumentoDeLaPersona)>0 and COUNT(Nombre) >0) a

                    left join
                    (select count(aparicionesID) as prueba, idAfiliado as identifAfi from (select*from(select idAfiliado, case when count(idAfiliado) >1 then idAfiliado end as aparicionesID from afiliados-pensionados-prote.afiliados_pensionados.PenFutura
                    group by idAfiliado
                    having count(idAfiliado)>0) where aparicionesID is not null
                    )
                    group by identifAfi
                    having count(aparicionesID)>1
                    ) as vg
                    on
                    vg.identifAfi=a.DocumentoDeLaPersona


                    left join
                    (select * from (select count(aparicionesModalidad) as pruebamodalidad,aparicionesModalidad, idAfiliado as identifAfi from (select*from(select a.MODALIDAD,idAfiliado, case when count(MODALIDAD) >0 then MODALIDAD end as aparicionesModalidad 
                    from afiliados-pensionados-prote.afiliados_pensionados.PenFutura a
                    group by MODALIDAD,idAfiliado
                    having count(MODALIDAD)>0) )
                    group by idAfiliado,aparicionesModalidad
                    having count(aparicionesModalidad)>1)
                    )ff
                    on
                    ff.identifAfi=a.DocumentoDeLaPersona


                ''',\
            use_standard_sql=True))




    dimPensionados_Dict = dimPensionados | beam.Map(lambda x: \
                                                            {'FechaDelDato':str(x['FechaDelDato']),\
                                                             'PensionadosID':str(x['PensionadosID']),\
                                                             'ModalidadPension':str(x['ModalidadPension']),\
                                                             'IndicadorPensionGPM':str(x['IndicadorPensionGPM']),\
                                                             'FechaNacimiento':str(x['FechaNacimiento']),\
                                                             'NumeroSolicitudPension':'NumeroSolicitudPension',\
                                                             'tipoDePension':str(x['tipoDePension']),\
                                                             'EstadoPension':str(x['EstadoPension']),\
                                                             'ModalidadInicial':str(x['ModalidadInicial']),\
                                                             'DocumentoDeLaPersona':str(x['DocumentoDeLaPersona']),\
                                                             'TipoDocumentoDeLaPersona':str(x['TipoDocumentoDeLaPersona']),\
                                                             'TipoDePersona':"Afiliados",\
                                                             'FechaPension':str(x['FechaPension']),\
                                                             'FechaSiniestro':str(x['FechaSiniestro']),\
                                                             'Sexo':str(x['Sexo']),\
                                                             'Nombre':str(x['Nombre']),\
                                                             'Mesadas':str(x['Mesadas']),\
                                                             'PENCON':str(x['PENCON']),\
                                                             'VejezAnticipada':str(x['VejezAnticipada']),\
                                                             'prueba':str(x['prueba']),\
                                                             'validacionDetected':""})



    #. validaciones  identificacion

    Identificacion_fullness_validated = dimPensionados_Dict | 'completitud identificacion' >> beam.Map(fn_check_completitud,    'DocumentoDeLaPersona' )

    Identificacion_numbers_validated = Identificacion_fullness_validated | 'solo numeros identificacion' >> beam.Map( fn_check_numbers,  'DocumentoDeLaPersona')

    TipoDocumentoDeLaPersona_validated = Identificacion_numbers_validated | 'completitud TipoDocumentoDeLaPersona' >> beam.Map(fn_check_completitud,    'TipoDocumentoDeLaPersona' )

    IdentificacionBeneficiario_qualityRule30_newNumeration = TipoDocumentoDeLaPersona_validated | 'repeated identification with different identification document type' >> beam.Map(fn_repeatedId_different_docType)

    #. validaciones ModalidadInicial

    ModalidadInicial_fullness_validated = IdentificacionBeneficiario_qualityRule30_newNumeration | 'completitud ModalidadInicial' >> beam.Map(fn_check_completitud,   'ModalidadInicial' )


    #. fecha pension

    FechaPension_fullness_validated = ModalidadInicial_fullness_validated | 'completitud FechaPension' >> beam.Map(fn_check_completitud,   'FechaPension')



    #. Fecha Siniestro

    FechaSiniestro_fullness_validated = FechaPension_fullness_validated | 'completitud FechaSiniestro' >> beam.Map(fn_check_completitud,   'FechaSiniestro')

    FechaSiniestro_QualityRule_8_checked  = FechaSiniestro_fullness_validated | 'birthdate cannot be greater than sinister day' >> beam.Map(fn_check_date_compare,   'FechaSiniestro', "%Y%m%d", "FechaNacimiento", "%Y-%m-%d")

    #FechaSiniestro_QualityRule_8_checked = FechaSiniestro_fullness_validated | 'regla 8 validada, FechaSiniestro  debe ser menor a fecha nacimiento ' >> beam.Map(fn_check_completitud,   'FechaSiniestro')

    #. Estado Pension

    EstadoPension_fullness_validated = FechaSiniestro_QualityRule_8_checked | 'completitud EstadoPension' >> beam.Map(fn_check_completitud,   'EstadoPension')

    #validaciones modalidaPension     

    ModalidadPension_fullness_validated = EstadoPension_fullness_validated | 'completitud modalidaPension' >> beam.Map(fn_check_completitud,   'ModalidadPension')

    ModalidadPension_conformidad_validated = ModalidadPension_fullness_validated | 'conformidad, debe ser uno de los 7 valores' >> beam.Map(fn_check_value_in,   'ModalidadPension', ["1","2","3","4","5","6","7"])

    ModalidadPension_RetiroProgramado_casoGPM =  ModalidadPension_conformidad_validated | 'Los casos GPM deben tener tipo de pensión retiro programado' >> beam.Map(fn_gcpm_case_different_program_retire)

    #. Indicador Pension GPM

    IndicadorPensionGPM_fullness_validated = ModalidadPension_RetiroProgramado_casoGPM | 'completitud IndicadorPensionGPM' >> beam.Map(fn_check_completitud,   'IndicadorPensionGPM')

    IndicadorPensionGPM_less_than_1150_weeks = IndicadorPensionGPM_fullness_validated | 'Pensiones de Garantia de Pension Minima y 1150 semanas' >>  beam.Map(fn_gpm_less_than_1150)

    IndicadorPensionGPM_age_to_fechaSiniestro = IndicadorPensionGPM_less_than_1150_weeks | 'edad 62 en hombre, 57 en mujeres al momento de la fecha de siniestro para casos GPM' >>  beam.Map(fn_age_to_fechaSiniestro)


    #, Mesadas

    Mesadas_fullness_validated = IndicadorPensionGPM_age_to_fechaSiniestro | 'completitud Mesadas' >> beam.Map(fn_check_completitud,   'Mesadas')

    Mesadas_onlyNumbers_validated = Mesadas_fullness_validated | 'solo numeros Mesadas' >> beam.Map(fn_check_numbers,   'Mesadas')

    Mesadas_valueInList_validated = Mesadas_onlyNumbers_validated | 'value in Mesadas' >> beam.Map(fn_check_value_in,   'Mesadas',["12.0","13.0","14.0"]) 


    #. FechaNacimiento

    FechaNacimiento_fullness_validated = Mesadas_valueInList_validated | 'completitud FechaNacimiento' >> beam.Map(fn_check_completitud,   'FechaNacimiento')

    FechaNacimiento_QualityRule_10_checked = FechaNacimiento_fullness_validated | 'pension before 62 men 57 women' >> beam.Map(fn_check_pension_oldeness_women_men)

    FechaNacimiento_QualityRule_46_checked = FechaNacimiento_QualityRule_10_checked | 'pension oldeness after 95' >> beam.Map(fn_check_pension_oldeness)

    # validaciones sexo

    Sexo_fullness_validated = FechaNacimiento_QualityRule_46_checked | 'completitud sexo' >> beam.Map( fn_check_completitud,  'Sexo')

    Sexo_text_validated = Sexo_fullness_validated | 'solo texto sexo' >> beam.Map( fn_check_text, 'Sexo')

    # validaciones Nombre

    Nombre_fullness_validated = Sexo_text_validated | 'completitud Nombre' >> beam.Map(fn_check_completitud,   'Nombre')

    FechaDelDato_fullness_validated = Nombre_fullness_validated | 'completitud FechaDelDato' >> beam.Map(fn_check_completitud,   'FechaDelDato')

    PensionadosID_fullness_validated = FechaDelDato_fullness_validated | 'completitud PensionadosID' >> beam.Map(fn_check_completitud,   'PensionadosID')

    NumeroSolicitudPension_fullness_validated = PensionadosID_fullness_validated | 'completitud NumeroSolicitudPension' >> beam.Map(fn_check_completitud,   'NumeroSolicitudPension')

    tipoDePension_fullness_validated = NumeroSolicitudPension_fullness_validated | 'completitud tipoDePension' >> beam.Map(fn_check_completitud,   'tipoDePension')

    FechaPension_dateFormat_validated = tipoDePension_fullness_validated | 'date format FechaPension' >> beam.Map(fn_check_date_format,    'FechaPension', "%Y%m%d" )

    FechaPension_biggerThan_validated = FechaPension_dateFormat_validated | 'bigger than FechaPension' >> beam.Map(fn_check_bigger_than,    'FechaPension', 19940401)

    RentaVitalicia_PENCON_VALIDATED = FechaPension_biggerThan_validated | 'rentaVitalicia, es decir modalidad pension de pension 2 debe ser PENCON 24'  >> beam.Map(fn_rentavitalicia_pencon_24)

    afiliadoDevolucion_totalSemanas = RentaVitalicia_PENCON_VALIDATED | 'afiliado con Devoluciones de Saldo no debe tener semanas en cero'  >> beam.Map(fn_total_weeks_afiliafos_devoluciones)

    afiliadoDevolucion_totalSemanas_vejez = afiliadoDevolucion_totalSemanas | 'afiliado con Devoluciones de Saldo no debe tener semanas en cero'  >> beam.Map(fn_total_weeks_afiliafos_devoluciones_vejez)

    # FechaNacimiento_o_QualityRule_64_checked = 


    # fn_check_pension_tipoPension_2

    



    results = afiliadoDevolucion_totalSemanas_vejez | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results["Clean"] | beam.Map(lambda x: {'FechaDelDato':str(x['FechaDelDato']),\
                                                     'PensionadosID':str(x['PensionadosID']),\
                                                     'ModalidadPension':str(x['ModalidadPension']),\
                                                     'IndicadorPensionGPM':str(x['IndicadorPensionGPM']),\
                                                     'FechaNacimiento':str(x['FechaNacimiento']),\
                                                     'NumeroSolicitudPension':" ",\
                                                     'tipoDePension':str(x['tipoDePension']),\
                                                     'EstadoPension':str(x['EstadoPension']),\
                                                     'ModalidadInicial':str(x['ModalidadInicial']),\
                                                     'DocumentoDeLaPersona':str(x['DocumentoDeLaPersona']),\
                                                     'TipoDocumentoDeLaPersona':str(x['TipoDocumentoDeLaPersona']),\
                                                     'TipoDePersona':str(x['TipoDePersona']),\
                                                     'FechaPension':str(x['FechaPension']),\
                                                     'FechaSiniestro':str(x['FechaSiniestro']),\
                                                     'Sexo':str(x['Sexo']),\
                                                     'Nombre':str(x['Nombre']),\
                                                     'Mesadas':str(x['Mesadas'])})



    dirty_ones = results["validationsDetected"] | beam.Map(lambda x: {'FechaDelDato':str(x['FechaDelDato']),\
                                                     'PensionadosID':str(x['PensionadosID']),\
                                                     'ModalidadPension':str(x['ModalidadPension']),\
                                                     'IndicadorPensionGPM':str(x['IndicadorPensionGPM']),\
                                                     'FechaNacimiento':str(x['FechaNacimiento']),\
                                                     'NumeroSolicitudPension':" ",\
                                                     'tipoDePension':str(x['tipoDePension']),\
                                                     'EstadoPension':str(x['EstadoPension']),\
                                                     'ModalidadInicial':str(x['ModalidadInicial']),\
                                                     'DocumentoDeLaPersona':str(x['DocumentoDeLaPersona']),\
                                                     'TipoDocumentoDeLaPersona':str(x['TipoDocumentoDeLaPersona']),\
                                                     'TipoDePersona':str(x['TipoDePersona']),\
                                                     'FechaPension':str(x['FechaPension']),\
                                                     'FechaSiniestro':str(x['FechaSiniestro']),\
                                                     'Sexo':str(x['Sexo']),\
                                                     'Nombre':str(x['Nombre']),\
                                                     'Mesadas':str(x['Mesadas']),\
                                                     'validacionDetected':str(x['validacionDetected'])})


    dirty_ones | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_dimPensionados_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimPensionados,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    result = p.run()
    result.wait_until_finish()
