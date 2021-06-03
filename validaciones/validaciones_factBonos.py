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


class ValidadorfactBonosLogic():
    def __init__(self, config):
        self._vName = self.__class__.__name__
        self._vConfig = config

    @staticmethod
    def fn_check_completitud(element,key):
        if (element[key] is None  or str(element[key]).lower() == "none" or (element[key]).lower() == "null" or str(element[key]).replace(" ","") == ""):
            element["validacionDetected"] = element["validacionDetected"] + "valor "+ str(key) +" no encontrado,"
        return element


    @staticmethod
    def fn_check_bigger_than_other(element,key1,key2):
        if (element[key1] is not None  and \
            element[key1] != "None" and \
            element[key1] != "null") and \
            element[key1].strip().replace(",","").replace(".","").isnumeric() and \
            element[key2] is not None  and \
            element[key2] != "None" and \
            element[key2] != "null" and \
            element[key2].strip().replace(",","").replace(".","").isnumeric():
            if float(element[key1].strip().replace(",","").replace(".","")) < \
               float(element[key2].strip().replace(",","").replace(".","")):
                element["validacionDetected"] = element["validacionDetected"] + "valor en "+ str(key1) +" es menor que "+str(key2)+","
        return element



    @staticmethod
    def fn_devoSaldo_lesser_than(element):
        if (element["CAI"] is not None  and \
            element["CAI"] != "None" and \
            element["CAI"] != "null") and \
            element["CAI"].strip().replace(",","").replace(".","").isnumeric() and \
            element["DevolucionSaldos"] is not None  and \
            element["DevolucionSaldos"] != "None" and \
            element["DevolucionSaldos"] != "null" and \
            element["DevolucionSaldos"].strip().replace(",","").replace(".","").isnumeric() and \
            element["Bono"] is not None  and \
            element["Bono"] != "None" and \
            element["Bono"] != "null" and \
            element["Bono"].strip().replace(",","").replace(".","").isnumeric():
            if float(element["DevolucionSaldos"].strip().replace(",","").replace(".",""))  > \
               (float(element["CAI"].strip().replace(",","").replace(".","")) + \
                float(element["Bono"].strip().replace(",","").replace(".",""))):
                element["validacionDetected"] = element["validacionDetected"] + "devolucion de saldo  es mayor a la suma de bono y saldo CAI,"
        return element


    @staticmethod
    def fn_devoSaldo_lesser_than_bono(element):
        if (element["DevolucionSaldos"] is not None  and \
            element["DevolucionSaldos"] != "None" and \
            element["DevolucionSaldos"] != "null" and \
            element["DevolucionSaldos"].strip().replace(",","").replace(".","").isnumeric() and \
            element["Bono"] is not None  and \
            element["Bono"] != "None" and \
            element["Bono"] != "null" and \
            element["Bono"].strip().replace(",","").replace(".","").isnumeric()):
            if float(element["DevolucionSaldos"].strip().replace(",","").replace(".",""))  < \
               float(element["Bono"].strip().replace(",","").replace(".","")):
                element["validacionDetected"] = element["validacionDetected"] + "devolucion de saldo  es menor al bono,"
        return element



    @staticmethod
    def fn_bono_sin_fecha_reden(element):
        if (element["FechaRedencionBono"] is  None  or \
            element["FechaRedencionBono"] == "None" or \
            element["FechaRedencionBono"] == "null") and \
            element["Bono"] is not None  and \
            element["Bono"] != "None" and \
            element["Bono"] != "null" and \
            element["EstadoBono"] is not None  and \
            element["EstadoBono"] != "None" and \
            element["EstadoBono"] != "null" :
            if str(element["EstadoBono"])  == "EMI" or  \
               str(element["EstadoBono"])  == "RED" or  \
               str(element["EstadoBono"])  == "CUS":
                element["validacionDetected"] = element["validacionDetected"] + "Bono con informacion y estado EMI, RED o CUS sin fecha de rendicion,"
        return element        

table_schema_factBonos = {
    'fields': [{
        'name':'BonoId', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Bono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoBono', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'EmisorBono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'CuponesBono', 'type':'STRING', 'mode':'NULLABLE'},                        
        {
        'name':'TasaRentabilidad', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaRedencionBono', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'FaltanteBonoPensional', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'BonoFechaTraslado', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaTrasladoBono', 'type':'STRING', 'mode':'NULLABLE'},               
        {
        'name':'VersionBono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SolicitudesId', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaLiquidacionBono', 'type':'STRING', 'mode':'NULLABLE'},                
        {
        'name':'CalculosActuarialesId', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}

table_schema_factBonos_malos = {
    'fields': [{
        'name':'BonoId', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'InfPersonasID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaDato', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'Bono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EstadoBono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'EmisorBono', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'CuponesBono', 'type':'STRING', 'mode':'NULLABLE'},                       
        {
        'name':'TasaRentabilidad', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaRedencionBono', 'type':'STRING', 'mode':'NULLABLE'}, 
        {
        'name':'FaltanteBonoPensional', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'VersionBono', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'SolicitudesId', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'FechaTrasladoBono', 'type':'STRING', 'mode':'NULLABLE'},        
        {
        'name':'BonoFechaTraslado', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'FechaLiquidacionBono', 'type':'STRING', 'mode':'NULLABLE'},                
        {
        'name':'CalculosActuarialesId', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'validacionDetected', 'type':'STRING', 'mode':'NULLABLE'}
        ]
}


config = configparser.ConfigParser()
config.read('/home/airflow/gcs/data/repo/configs/config.ini')
validador = ValidadorfactBonosLogic(config)
options1 = PipelineOptions(
argv= None,
runner=config['configService']['runner'],
project=config['configService']['project'],
job_name='factbonos-apache-beam-job-name',
temp_location=config['configService']['temp_location'],
region=config['configService']['region'],
service_account_email=config['configService']['service_account_email'],
save_main_session= config['configService']['save_main_session'])


table_spec_clean = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_factBonos')


table_spec_dirty = bigquery.TableReference(
    projectId=config['configService']['project'],
    datasetId='Datamart',
    tableId='DATAMART_PENSIONADOS_factBonos_dirty') 

with beam.Pipeline(options=options1) as p:
   
        
        
    factBonos  = (
        p
        | 'Query Table Clientes' >> beam.io.ReadFromBigQuery(
            query='''
                            WITH Bonos AS (SELECT penret.AFI_HASH64,
                            GENERATE_UUID() AS BonoId,
                            'None' as Inf_PersonasId,
                            CURRENT_DATE() AS FechaDato,
                            BONO as Bono,
                            TASA AS TasaRentabilidad,
                            FR_BONO AS FechaRedencionBono,
                            'None' AS FaltanteBonoPensional, -- de donde se saca este dato?
                            ESTADO_CUPON EstadoBono,
                            '' EmisorBono,
                            VERSION_BONO as VersionBono,
                            '' CalculosActuarialesId,
                            CONSECUTIVO_CUPON CuponesBono,
                            '' SolicitudesId,
                            'None' BonoFechaTraslado, -- de donde se saca este dato?
                            'None' FechaTrasladoBono, -- de donde se saca este dato?
                            'None' FechaLiquidacionBono -- de donde se saca este dato?
                            FROM `'''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_SUPERBONO2` supb
                            LEFT JOIN 
                            (SELECT DISTINCT AFI_HASH64, BONO, cast(FR_BONO as string) FR_BONO  FROM
                            `'''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_pensionadosFutura`
                            UNION DISTINCT
                            SELECT DISTINCT AFI_HASH64, BONO, 'None' FR_BONO FROM
                            `'''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_Retiros`
                            ) penret
                            ON
                            supb.AFI_HASH64=penret.AFI_HASH64
                            LEFT JOIN
                            '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_SUPERCUPON cup
                            ON
                            cup.AFI_HASH64=penret.AFI_HASH64)

                            ,EMISOR AS 
                            (SELECT TipoIdEntidadEmisora, IdEntidadEmisora, EMP_HASH64, AFI_HASH64
                            FROM (
                            SELECT DISTINCT TipoIdEntidadEmisora, IdEntidadEmisora, ENTIDAD.EMP_HASH64, AFI_HASH64 
                            FROM 
                            (SELECT EZ51.tipoIdEmpleador TipoIdEntidadEmisora, EZ51.idEmpleador IdEntidadEmisora, E.EMP_HASH64
                            FROM '''+config['configService']['datasetDatalake']+'''.AS400_FPOBLIDA_ENTAR9 E INNER JOIN
                            '''+config['configService']['datasetDatalake']+'''.SQLSERVER_BIPROTECCIONDW_DW_DIMEMPLEADORES_Z51 EZ51
                            ON E.EMP_HASH64=EZ51.EMP_HASH64) ENTIDAD

                            LEFT JOIN 
                            '''+config['configService']['Datawarehouse']+'''.DATAMART_PENSIONADOS_SUPERBONO2 SB
                            ON 
                            ENTIDAD.EMP_HASH64=SB.EMP_HASH64)
                            ), info as (

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
                            )
                            )
                            SELECT BonoId, info.InfPersonasID, bonos.FechaDato, Bono, TasaRentabilidad, FechaRedencionBono, FaltanteBonoPensional,
                            EstadoBono,IdEntidadEmisora as EmisorBono, VersionBono, CalculosActuarialesId,
                            CuponesBono, soli.SolicitudesId, BonoFechaTraslado, FechaTrasladoBono, FechaLiquidacionBono
                            FROM Bonos
                            LEFT JOIN 
                            EMISOR
                            ON 
                            CAST(Bonos.AFI_HASH64 AS STRING) = CAST(EMISOR.AFI_HASH64 AS STRING)
                            LEFT JOIN 
                            info
                            ON
                            info.AFI_HASH64=Bonos.AFI_HASH64
                            LEFT JOIN 
                            '''+config['configService']['Datamart']+'''.DATAMART_PENSIONADOS_factSolicitudes soli
                            ON
                            info.InfPersonasID=soli.InfPersonasID


                ''',\
            use_standard_sql=True))

                                            # 'CAI':str(x['CAI']),\}
                                            # 'DevolucionSaldos':str(x['DevolucionSaldos']),\

    factBonos_Dict = factBonos | beam.Map(lambda x: {'BonoId':str(x['InfPersonasID']),\
                                                            'InfPersonasID':str(x['InfPersonasID']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'Bono':str(x['Bono']),\
                                                            'TasaRentabilidad':str(x['TasaRentabilidad']),\
                                                            'EstadoBono':str(x['EstadoBono']),\
                                                            'EmisorBono':str(x['EmisorBono']),\
                                                            'SolicitudesId':str(x['SolicitudesId']),\
                                                            'FechaTrasladoBono':str(x['FechaTrasladoBono']),\
                                                            'BonoFechaTraslado':str(x['BonoFechaTraslado']),\
                                                            'FechaRedencionBono':str(x['FechaRedencionBono']),\
                                                            'FaltanteBonoPensional':str(x['FaltanteBonoPensional']),\
                                                            'FechaLiquidacionBono':str(x['FechaLiquidacionBono']),\
                                                            'VersionBono':str(x['VersionBono']),\
                                                            'CalculosActuarialesId':str(x['CalculosActuarialesId']),\
                                                            'validacionDetected':""})


    InfPersonasID_fullness_validated = factBonos_Dict | 'completitud InfPersonasID' >> beam.Map(validador.fn_check_completitud,    'InfPersonasID' )

    FechaDato_fullness_validated = InfPersonasID_fullness_validated | 'completitud FechaDato' >> beam.Map(validador.fn_check_completitud,    'FechaDato' )

    TasaRentabilidad_fullness_validated = FechaDato_fullness_validated | 'completitud TasaRentabilidad' >> beam.Map(validador.fn_check_completitud,    'TasaRentabilidad' )

    FechaRedencionBono_fullness_validated = TasaRentabilidad_fullness_validated | 'completitud FechaRedencionBono' >> beam.Map(validador.fn_check_completitud,    'FechaRedencionBono' )

    Bono_fullness_validated = FechaRedencionBono_fullness_validated | 'completitud Bono' >> beam.Map(validador.fn_check_completitud,    'Bono' )

    FaltanteBonoPensional_fullness_validated = Bono_fullness_validated | 'completitud FaltanteBonoPensional' >> beam.Map(validador.fn_check_completitud,    'FaltanteBonoPensional' )

    VersionBono_fullness_validated = FaltanteBonoPensional_fullness_validated | 'completitud VersionBono' >> beam.Map(validador.fn_check_completitud,    'VersionBono' )

    CalculosActuarialesId_fullness_validated = VersionBono_fullness_validated | 'completitud CalculosActuarialesId' >> beam.Map(validador.fn_check_completitud,    'CalculosActuarialesId' )

    # DevolucionSaldos_biggerThan_Bono = CalculosActuarialesId_fullness_validated  | 'DevolucionSaldos CuponesBonoID' >> beam.Map(validador.fn_check_bigger_than_other, 'Bono','DevolucionSaldos')

    # DevolucionSaldos_lesserThan_combined = DevolucionSaldos_biggerThan_Bono  | 'DevolucionSaldos less than Bono and saldo CAI' >> beam.Map(validador.fn_devoSaldo_lesser_than)

    # DevolucionSaldos_lesserThan_bono = DevolucionSaldos_lesserThan_combined    | 'DevolucionSaldos less than Bono' >> beam.Map(validador.fn_devoSaldo_lesser_than_bono)

    bonoSinFechaRedencion_validation = CalculosActuarialesId_fullness_validated    | 'Bono estado EMI, RED o CUS sin fecha de rendicion' >> beam.Map(validador.fn_bono_sin_fecha_reden)

    results = bonoSinFechaRedencion_validation | beam.ParDo(fn_divide_clean_dirty()).with_outputs()


    limpias = results['Clean'] | beam.Map(lambda x:         {'BonoId':str(x['InfPersonasID']),\
                                                            'InfPersonasID':str(x['InfPersonasID']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'Bono':str(x['Bono']),\
                                                            'EstadoBono':str(x['EstadoBono']),\
                                                            'EmisorBono':str(x['EmisorBono']),\
                                                            'SolicitudesId':str(x['SolicitudesId']),\
                                                            'TasaRentabilidad':str(x['TasaRentabilidad']),\
                                                            'FechaTrasladoBono':str(x['FechaTrasladoBono']),\
                                                            'BonoFechaTraslado':str(x['BonoFechaTraslado']),\
                                                            'FechaRedencionBono':str(x['FechaRedencionBono']),\
                                                            'FaltanteBonoPensional':str(x['FaltanteBonoPensional']),\
                                                            'FechaLiquidacionBono':str(x['FechaLiquidacionBono']),\
                                                            'VersionBono':str(x['VersionBono']),\
                                                            'CalculosActuarialesId':str(x['CalculosActuarialesId'])})



    validadas = results['validationsDetected'] | beam.Map(lambda x:         {'BonoId':str(x['InfPersonasID']),\
                                                            'InfPersonasID':str(x['InfPersonasID']),\
                                                            'FechaDato':str(x['FechaDato']),\
                                                            'Bono':str(x['Bono']),\
                                                            'EstadoBono':str(x['EstadoBono']),\
                                                            'EmisorBono':str(x['EmisorBono']),\
                                                            'SolicitudesId':str(x['SolicitudesId']),\
                                                            'TasaRentabilidad':str(x['TasaRentabilidad']),\
                                                            'FechaTrasladoBono':str(x['FechaTrasladoBono']),\
                                                            'BonoFechaTraslado':str(x['BonoFechaTraslado']),\
                                                            'FechaRedencionBono':str(x['FechaRedencionBono']),\
                                                            'FaltanteBonoPensional':str(x['FaltanteBonoPensional']),\
                                                            'FechaLiquidacionBono':str(x['FechaLiquidacionBono']),\
                                                            'VersionBono':str(x['VersionBono']),\
                                                            'CalculosActuarialesId':str(x['CalculosActuarialesId']),\
                                                            'validacionDetected':str(x['validacionDetected'])})


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







    validadas | "write validated ones" >> beam.io.WriteToBigQuery(
            table_spec_dirty,
            schema=table_schema_factBonos_malos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    limpias |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_factBonos,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            


