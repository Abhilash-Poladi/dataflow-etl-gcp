import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings(
    "ignore",
    message="crcmod package not found"
)
import csv
from apache_beam.io.parquetio import WriteToParquet
import pyarrow as pa
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from io import StringIO/

from apache_beam.io import BigQueryDisposition
from apache_beam.io.gcp.bigquery import WriteToBigQuery


class ParseCSV(beam.DoFn):
    def process(self, element):
        line = next(csv.reader(StringIO(element)))
        yield {
               'name' : line[0],
              'age' : int(line[1]) 
              }

class Parseline(beam.DoFn):
    def process(self, element):
        elements = element.split(',',6)
        yield { 'age_bracket': elements[0],'id': elements[1], 'ip': elements[2], 'lat': float(elements[3]),
                'lng': float(elements[4]), 'opted_into_marketing': elements[5].lower()=='true', 'user_agent': elements[6]        
        }

table_schema = {
    "fields": [
        {"name": "age_bracket", "type": "STRING", "mode": "NULLABLE"},
        {"name": "id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ip", "type": "STRING", "mode": "NULLABLE"},
        {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "lng", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "opted_into_marketing", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "user_agent", "type": "STRING", "mode": "NULLABLE"},
    ]
}        

parquet_schema = pa.schema([
    ("name", pa.string()),
    ("age", pa.int32())
])

options = PipelineOptions()

with beam.Pipeline(options=options) as p:
    (
        p
        | ReadFromText("users.txt", skip_header_lines=1)
        #| beam.Map(lambda x: print(type(x)))
        | beam.ParDo(Parseline())
        | WriteToBigQuery(
        table="project-26fb084d-6d96-4ec6-8a7.my_dataset.my_table",
        schema=table_schema,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        custom_gcs_temp_location="gs://bq-stagingpol/temp"
        # | beam.Map(lambda x : (x['id'],1))
        # | beam.CombinePerKey(sum)
        # | beam.Map(print)
)
    