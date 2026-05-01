import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import csv
from apache_beam.io.parquetio import WriteToParquet
import pyarrow as pa
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from io import StringIO
from apache_beam.io import BigQueryDisposition
from apache_beam.io.gcp.bigquery import WriteToBigQuery



class Parseline(beam.DoFn):
    def process(self, element):
        elements = element.split(',',6)
        yield { 'age_bracket': elements[0],'id': elements[1], 'ip': elements[2], 'lat': float(elements[3]),
                'lng': float(elements[4]), 'opted_into_marketing': elements[5].lower()=='true', 'user_agent': elements[6]        
        }

class Parseloc(beam.DoFn):
    def process(self, element):
        elements = element.split(',',6)
        yield {"id": elements[0], "city": elements[1], "country": elements[2], "last_login_ip": elements[3],
              "last_login_ts": elements[4]}
    

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

options = PipelineOptions()

with beam.Pipeline(options=options) as p:
    raw_data = (
        p
        | ReadFromText("users.txt", skip_header_lines=1)
        | beam.ParDo(Parseline())
        | beam.Map(lambda x : (x['id'],x))
    )

    locations_data = (
        p           
        | "readfromlocation" >> ReadFromText("locations.txt", skip_header_lines=1)
        | "locationpardo" >> beam.ParDo(Parseloc())
        | "print" >> beam.Map(lambda x: (x['id'],x))
    )

    joined_data = (
        {
            "raw" : raw_data,
            "locations" : locations_data
        }
            | beam.CoGroupByKey()
            | beam.Filter(lambda x: len(x[1]['locations'])!=0)
            | beam.Map(lambda x : (x[0], x[1]['raw'][0]['age_bracket'], x[1]['locations'][0]['city']))
            | beam.Map(print)
    )
    