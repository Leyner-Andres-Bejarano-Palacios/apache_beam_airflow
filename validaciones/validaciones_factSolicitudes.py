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


  
table_schema_factSolicitudes = {
    'fields': [
        {
        'name':'SolicitudesID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'NumeroSolicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoSolicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoSolicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaSiniestro', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DepartamentoSiniestro', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CiudadSiniestro', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CausalSiniestro', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaInicioVigencia', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'IndicadorRegistroActivo', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaNacimiento', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'FechaSolicitud', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_factSolicitudes_malos = {
    'fields': [
        {
        'name':'SolicitudesID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'NumeroSolicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoSolicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'TipoSolicitud', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaSiniestro', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'DepartamentoSiniestro', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CiudadSiniestro', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CausalSiniestro', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaInicioVigencia', 'type':'STRING', 'mode':'NULLABLE'},       
        {
        'name':'IndicadorRegistroActivo', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaNacimiento', 'type':'STRING', 'mode':'NULLABLE'},         
        {
        'name':'FechaSolicitud', 'type':'STRING', 'mode':'NULLABLE'},                      
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}


config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
validador = ValidadorfactSolicitudesLogic(config)
options1 = PipelineOptions(
argv= None,
runner=config['configService']['runner'],
project=config['configService']['project'],
job_name='factsolicitudes-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_factSolicitudes')


table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_factSolicitudes_dirty')

with beam.Pipeline(options=options1) as p:
    dimTipoSolicitud  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''

                    with solicitudes as ((SELECT GENERATE_UUID() AS SolicitudesID,
                    'None' TiempoID,
                    NUMERO_SOLICITUD AS NumeroSolicitud,
                    ESTADO_SOLICITUD AS EstadoSolicitud,
                    TIPO_SOLICITUD AS TipoSolicitud,
                    'None' InfPersonasID,
                    FECHA_SINIESTRO AS FechaSiniestro,
                    'None' DepartamentoSiniestro,
                    'None' CiudadSiniestro,
                    CURRENT_DATE() FechaDato,
                    CAUSAL_SINIESTRO AS CausalSiniestro,
                    'None' FechaInicioVigencia,
                    'None' FechaFinVigencia,
                    'None' IndicadorRegistroActivo,
                    FECHA_SOLICITUD AS FechaSolicitud,
                    AFI_HASH64
                    FROM 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_solicitudes)

                    UNION DISTINCT 
                    (
                    SELECT GENERATE_UUID() AS SolicitudesID,
                    'None' TiempoID,
                    pen.NUMERO_SOLICITUD AS NumeroSolicitud,
                    sol.ESTADO_SOLICITUD AS EstadoSolicitud,
                    pen.TIPO_SOLICITUD AS TipoSolicitud,
                    'None' InfPersonasID,
                    pen.FECHA_SINIESTRO AS FechaSiniestro,
                    'None' DepartamentoSiniestro,
                    'None' CiudadSiniestro,
                    CURRENT_DATE() FechaDato,
                    CAUSAL_SINIESTRO AS CausalSiniestro,
                    'None' FechaInicioVigencia,
                    'None' FechaFinVigencia,
                    'None' IndicadorRegistroActivo,
                    pen.FECHA_SOLICITUD AS FechaSolicitud, pen.AFI_HASH64
                    FROM 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_pensionadosFutura pen
                    LEFT JOIN 
                    '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_solicitudes sol
                    on   
                    pen.AFI_HASH64=sol.AFI_HASH64
                    ))
                    select SolicitudesID, TiempoID, NumeroSolicitud, EstadoSolicitud, TipoSolicitud,
                    info.InfPersonasID, FechaSiniestro, DepartamentoSiniestro, CiudadSiniestro, FechaDato,
                    CausalSiniestro, FechaInicioVigencia, IndicadorRegistroActivo, FechaSolicitud, dtmFechaNacimiento AS FechaNacimiento
                    FROM solicitudes

                    LEFT JOIN

                    (SELECT DISTINCT per_HASH64, dtmFechaNacimiento from
                    `'''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Apolo`) ap
                    ON 
                    ap.per_HASH64=solicitudes.AFI_HASH64

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
                    solicitudes.AFI_HASH64=info.AFI_HASH64

                ''',\
            use_standard_sql=True))





    dimTipoSolicitudDict = dimTipoSolicitud | beam.Map(lambda x: {'SolicitudesID':str(x['SolicitudesID']),\
                                                              'TiempoID':str(x['TiempoID']),\
                                                              'NumeroSolicitud':str(x['NumeroSolicitud']),\
                                                              'EstadoSolicitud':str(x['EstadoSolicitud']),\
                                                              'TipoSolicitud':str(x['TipoSolicitud']),\
                                                              'InfPersonasID':str(x['InfPersonasID']),\
                                                              'FechaSiniestro':str(x['FechaSiniestro']),\
                                                              'DepartamentoSiniestro':str(x['DepartamentoSiniestro']),\
                                                              'CiudadSiniestro':str(x['CiudadSiniestro']),\
                                                              'FechaDato':str(x['FechaDato']),\
                                                              'CausalSiniestro':str(x['CausalSiniestro']),\
                                                              'FechaInicioVigencia':str(x['FechaInicioVigencia']),\
                                                              'IndicadorRegistroActivo':str(x['IndicadorRegistroActivo']),\
                                                              'FechaNacimiento':str(x['FechaNacimiento']),\
                                                              'FechaSolicitud':str(x['FechaSolicitud']),\
                                                              'validacionDetected':""})


    FechaSiniestro_fullness_validated = dimTipoSolicitudDict | 'completitud FechaSiniestro' >> beam.Map(validador.fn_check_completitud,   'FechaSiniestro')

    FechaSiniestro_QualityRule_8_checked  = FechaSiniestro_fullness_validated | 'birthdate cannot be greater than sinister day' >> beam.Map(validador.fn_check_date_compare,   'FechaSiniestro', "%Y%m%d", "FechaNacimiento", "%Y%m%d")


    # TiempoID_fullness_validated = dimTipoSolicitud | 'completitud TiempoID' >> beam.Map(fn_check_completitud,    'TiempoID' )

    # EstadoID_fullness_validated = TiempoID_fullness_validated | 'completitud EstadoID' >> beam.Map(fn_check_completitud,    'EstadoID' )

    # Numero_Solicitud_fullness_validated = EstadoID_fullness_validated | 'completitud Numero_Solicitud' >> beam.Map(fn_check_completitud,    'Numero_Solicitud' )

    # TipoSolicitudID_fullness_validated = Numero_Solicitud_fullness_validated | 'completitud TipoSolicitudID' >> beam.Map(fn_check_completitud,    'TipoSolicitudID' )

    # InfPersonasID_fullness_validated = TipoSolicitudID_fullness_validated | 'completitud InfPersonasID' >> beam.Map(fn_check_completitud,    'InfPersonasID' )

    # FechaDato_fullness_validated = InfPersonasID_fullness_validated | 'completitud FechaDato' >> beam.Map(fn_check_completitud,    'FechaDato' )






    


    results = FechaSiniestro_QualityRule_8_checked | beam.ParDo(fn_divide_clean_dirty()).with_outputs()



    limpias = results["Clean"] | beam.Map(lambda x: {'SolicitudesID':str(x['SolicitudesID']),\
                                                              'TiempoID':str(x['TiempoID']),\
                                                              'NumeroSolicitud':str(x['NumeroSolicitud']),\
                                                              'EstadoSolicitud':str(x['EstadoSolicitud']),\
                                                              'TipoSolicitud':str(x['TipoSolicitud']),\
                                                              'InfPersonasID':str(x['InfPersonasID']),\
                                                              'FechaSiniestro':str(x['FechaSiniestro']),\
                                                              'DepartamentoSiniestro':str(x['DepartamentoSiniestro']),\
                                                              'CiudadSiniestro':str(x['CiudadSiniestro']),\
                                                              'FechaDato':str(x['FechaDato']),\
                                                              'CausalSiniestro':str(x['CausalSiniestro']),\
                                                              'FechaInicioVigencia':str(x['FechaInicioVigencia']),\
                                                              'IndicadorRegistroActivo':str(x['IndicadorRegistroActivo']),\
                                                              'FechaNacimiento':str(x['FechaNacimiento']),\
                                                              'FechaSolicitud':str(x['FechaSolicitud'])})



    dirty_ones = results["validacionDetected"] | beam.Map(lambda x: {'SolicitudesID':str(x['SolicitudesID']),\
                                                              'TiempoID':str(x['TiempoID']),\
                                                              'NumeroSolicitud':str(x['NumeroSolicitud']),\
                                                              'EstadoSolicitud':str(x['EstadoSolicitud']),\
                                                              'TipoSolicitud':str(x['TipoSolicitud']),\
                                                              'InfPersonasID':str(x['InfPersonasID']),\
                                                              'FechaSiniestro':str(x['FechaSiniestro']),\
                                                              'DepartamentoSiniestro':str(x['DepartamentoSiniestro']),\
                                                              'CiudadSiniestro':str(x['CiudadSiniestro']),\
                                                              'FechaDato':str(x['FechaDato']),\
                                                              'CausalSiniestro':str(x['CausalSiniestro']),\
                                                              'FechaInicioVigencia':str(x['FechaInicioVigencia']),\
                                                              'IndicadorRegistroActivo':str(x['IndicadorRegistroActivo']),\
                                                              'FechaNacimiento':str(x['FechaNacimiento']),\
                                                              'FechaSolicitud':str(x['FechaSolicitud']),\
                                                              'validacionDetected':str(x['validacionDetected'])})

    results["validacionDetected"] | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_factSolicitudes_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_factSolicitudes,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)



# # -*- coding: utf-8 -*-

# import uuid
# import time
# import types
# import threading
# import numpy as np
# import pandas as pd
# import apache_beam as beam
# from apache_beam import pvalue
# from apache_beam.runners.runner import PipelineState
# from apache_beam.io.gcp.internal.clients import bigquery
# from apache_beam.options.pipeline_options import PipelineOptions





# options1 = PipelineOptions(
#     argv= None,
#     runner='DataflowRunner',
#     project='afiliados-pensionados-prote',
#     job_name='factsolicitudes-apache-beam-job-name',
#     temp_location='gs://bkt_prueba/temp',
#     region='us-central1',
#     service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
#     save_main_session= 'True')


# table_spec_clean = bigquery.TableReference(
#     projectId=config['configService']['project'],
#     datasetId='Datamart',
#     tableId='DATAMART_PENSIONADOS_factSolicitudes')


# table_spec_dirty = bigquery.TableReference(
#     projectId=config['configService']['project'],
#     datasetId='Datamart',
#     tableId='DATAMART_PENSIONADOS_factSolicitudes_dirty')


  
# table_schema_factSolicitudes = {
#     'fields': [
#         {
#         'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
#         {
#         'name':'EstadoID', 'type':'STRING', 'mode':'NULLABLE'},
#         {
#         'name':'Numero_Solicitud', 'type':'STRING', 'mode':'NULLABLE'},
#         {
#         'name':'TipoSolicitudID', 'type':'STRING', 'mode':'NULLABLE'},
#         {
#         'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
#         {
#         'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
#         ]
# }

# table_schema_factSolicitudes_malos = {
#     'fields': [
#         {
#         'name':'TiempoID', 'type':'STRING', 'mode':'NULLABLE'},
#         {
#         'name':'EstadoID', 'type':'STRING', 'mode':'NULLABLE'},
#         {
#         'name':'Numero_Solicitud', 'type':'STRING', 'mode':'NULLABLE'},
#         {
#         'name':'TipoSolicitudID', 'type':'STRING', 'mode':'NULLABLE'},
#         {
#         'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
#         {
#         'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'},
#         ]
# }


# class fn_divide_clean_dirty(beam.DoFn):
#   def process(self, element):
#     correct = False
#     if element["validacionDetected"] == "":
#         correct = True
#         del element["validacionDetected"]

#     if correct == True:
#         yield pvalue.TaggedOutput('Clean', element)
#     else:
#         yield pvalue.TaggedOutput('validationsDetected', element)

# def fn_check_completitud(element,key):
#     if (element[key] is None  or element[key] == "None" or element[key] == "null"):
#         element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
#     return element





# if __name__ == "__main__":
#     p = beam.Pipeline(options=options1)
#     dimTipoSolicitud  = (
#         p
#         | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
#             query='''
#                 SELECT  "None" TiempoID, Numero_Solicitud, e.EstadoID, b.TipoSolicitudID, a.InfPersonasID, CURRENT_DATE() as FechaDato
#                 FROM

#                 '''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_dimInfPersonas a

#                 LEFT JOIN
#                 (SELECT PensionadosID, DocumentoDeLaPersona FROM
#                 '''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_dimPensionados c
#                 UNION ALL
#                 SELECT BeneficiariosID, IdentificacionAfiliado FROM
#                 '''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_dimBeneficiarios d
#                 ) f
#                 ON
#                 f.PensionadosID=a.PensionadosId

#                 LEFT JOIN
#                 '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_PenFutura pa
#                 ON
#                 pa.idAfiliado=f.DocumentoDeLaPersona

#                 LEFT JOIN
#                 '''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_dimTipoSolicitud b
#                 ON
#                 b.TipoSolicitud=pa.TIPO_SOLICITUD
#                 LEFT JOIN
#                 '''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_dimEstado e
#                 on
#                 e.Estado=pa.ESTADO_PENSION /*falta asociar el campo EstadoSolicitud*/
#                 ''',\
#             use_standard_sql=True))





#     dimTipoSolicitud = dimTipoSolicitud | beam.Map(lambda x: {'TiempoID':str(x['TiempoID']),\
#                                                               'EstadoID':str(x['EstadoID']),\
#                                                               'Numero_Solicitud':str(x['Numero_Solicitud']),\
#                                                               'TipoSolicitudID':str(x['TipoSolicitudID']),\
#                                                               'InfPersonasID':str(x['InfPersonasID']),\
#                                                               'FechaDato':str(x['FechaDato']),\
#                                                               'validacionDetected':""})




#     TiempoID_fullness_validated = dimTipoSolicitud | 'completitud TiempoID' >> beam.Map(fn_check_completitud,    'TiempoID' )

#     EstadoID_fullness_validated = TiempoID_fullness_validated | 'completitud EstadoID' >> beam.Map(fn_check_completitud,    'EstadoID' )

#     Numero_Solicitud_fullness_validated = EstadoID_fullness_validated | 'completitud Numero_Solicitud' >> beam.Map(fn_check_completitud,    'Numero_Solicitud' )

#     TipoSolicitudID_fullness_validated = Numero_Solicitud_fullness_validated | 'completitud TipoSolicitudID' >> beam.Map(fn_check_completitud,    'TipoSolicitudID' )

#     InfPersonasID_fullness_validated = TipoSolicitudID_fullness_validated | 'completitud InfPersonasID' >> beam.Map(fn_check_completitud,    'InfPersonasID' )

#     FechaDato_fullness_validated = InfPersonasID_fullness_validated | 'completitud FechaDato' >> beam.Map(fn_check_completitud,    'FechaDato' )






    


#     results = FechaDato_fullness_validated | beam.ParDo(fn_divide_clean_dirty()).with_outputs()



#     limpias = results["Clean"] | beam.Map(lambda x: {'TiempoID':str(x['TiempoID']),\
#                                                      'EstadoID':str(x['EstadoID']),\
#                                                      'Numero_Solicitud':str(x['Numero_Solicitud']),\
#                                                      'TipoSolicitudID':str(x['TipoSolicitudID']),\
#                                                      'InfPersonasID':str(x['InfPersonasID']),\
#                                                      'FechaDato':str(x['FechaDato'])})



#     results["validacionDetected"] | "write validated ones" >> beam.io.WriteToBigQuery(
#             table_spec_dirty,
#             schema=table_schema_factSolicitudes_malos,
#             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
#             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


#     limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
#             table_spec_clean,
#             schema=table_schema_factSolicitudes,
#             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
#             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
#     p.run()
