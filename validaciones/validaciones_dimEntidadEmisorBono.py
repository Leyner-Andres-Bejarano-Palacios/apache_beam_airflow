import uuid
import numpy as np
import pandas as pd
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions



options1 = PipelineOptions(
    argv= None,
    runner='DataflowRunner',
    project='afiliados-pensionados-prote',
    job_name='dimentidademisorbono-apache-beam-job-name',
    temp_location='gs://bkt_prueba/temp',
    region='us-central1',
    service_account_email='composer@afiliados-pensionados-prote.iam.gserviceaccount.com',
    save_main_session= 'True')


table_spec_clean = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimEntidadEmisorBono')


table_spec_dirty = bigquery.TableReference(
    projectId='afiliados-pensionados-prote',
    datasetId='Datamart',
    tableId='dimEntidadEmisorBono_dirty')


table_schema_dimEntidadEmisorBono = {
    'fields': [{
        'name': 'TipoIdEntidadEmisora', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'EntidadEmisoraBonoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IdEntidadEmisora', 'type':'STRING', 'mode':'NULLABLE'}       
        ]
}

table_schema_dimEntidadEmisorBono_malos = {
    'fields': [{
        'name': 'TipoIdEntidadEmisora', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name':'EntidadEmisoraBonoID', 'type':'STRING', 'mode':'NULLABLE'},
        {
        'name':'IdEntidadEmisora', 'type':'STRING', 'mode':'NULLABLE'},
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



if __name__ == "__main__":
    p = beam.Pipeline(options=options1)
    dimEntidadEmisorBono  = (
        
       p | 'Query Table dimEntidadEmisorBono' >> beam.io.ReadFromBigQuery(
            query='''
                SELECT GENERATE_UUID() as EntidadEmisoraBonoID, TipoIdEntidadEmisora, IdEntidadEmisora FROM (SELECT EZ51.tipoIdEmpleador TipoIdEntidadEmisora, EZ51.idEmpleador IdEntidadEmisora, E.EMP_HASH64
                FROM notional-radio-302217.DatalakeAnalitica.AS400_FPOBLIDA_ENTAR9 E INNER JOIN
                notional-radio-302217.DatalakeAnalitica.SQLSERVER_BIPROTECCIONDW_DW_DIMEMPLEADORES_Z51 EZ51
                ON E.EMP_HASH64=EZ51.EMP_HASH64)
                  ''',\
            use_standard_sql=True))


    dimEntidadEmisorBono_Dict = dimEntidadEmisorBono | beam.Map(lambda x: \
                                                            {'TipoIdEntidadEmisora':str(x['IdEntidadEmisora']),\
                                                             'EntidadEmisoraBonoID':str(x['EntidadEmisoraBonoID']),\
                                                             'IdEntidadEmisora':str(x['IdEntidadEmisora'])})





    dimEntidadEmisorBono_Dict |  "write clean ones" >>  beam.io.WriteToBigQuery(
            table_spec_clean,
            schema=table_schema_dimEntidadEmisorBono,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            
    p.run()
