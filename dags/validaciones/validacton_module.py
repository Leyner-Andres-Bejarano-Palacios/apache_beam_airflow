# -*- coding: utf-8 -*-
import uuid
import numpy as np
import pandas as pd
from datetime import date
import apache_beam as beam
from datetime import datetime
from apache_beam import pvalue
from google.cloud import bigquery as bq
from google.cloud.exceptions import NotFound
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions




def branch_func(**kwargs):
    client = bq.Client()
    sql = kwargs["templates_dict"]["query"]
    try:
        df = client.query(sql).to_dataframe()
        if len(df) > 0:
            return 'new_records_task'
        else:
            return 'new_table_task'
    except NotFound:
        return 'new_table_task'
