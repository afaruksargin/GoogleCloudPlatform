import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery

PROJECT_ID = 'jovial-evening-394610'
SCHEMA = 'sr:INTEGER,abv:FLOAT,id:INTEGER,name:STRING,style:STRING,ounces:FLOAT'

def discard_incomplete(data):
    return len(data['abv']) > 0 and len(data['id']) > 0 and len(data['name']) > 0 and len(data['style']) > 0

# Uygun veri dönüşümlerini yapıyoruz.
def convert_types(data):
    data['abv'] = float(data['abv']) if 'abv' in data else None
    data['id'] = int(data['id']) if 'id' in data else None
    data['name'] = str(data['name']) if 'name' in data else None
    data['style'] = str(data['style']) if 'style' in data else None
    data['ounces'] = float(data['ounces']) if 'ounces' in data else None
    return data

# İstemediğimiz değişkenleri kaldırıyoruz.
def del_unwanted_cols(data):
    del data['ibu']
    del data['brewery_id']
    return data


def run_pipeline():

    options = PipelineOptions([
        '--job_name=a',
        '--runner=DataflowRunner',
        '--project=jovial-evening-394610',
        '--region=us-central1',
        '--temp_location=gs://pipline-beam/temp',
        '--staging_location=gs://pipline-beam/staging'
    ])


    with beam.Pipeline(options=options) as p:

        pcollection = (
            p | 'ReadData' >> beam.io.ReadFromText('gs://pipline-beam/batch/beers.csv', skip_header_lines =1)
              | 'SplitData' >> beam.Map(lambda x: x.split(','))
              | 'FormatToDict' >> beam.Map(lambda x: {"sr": x[0], "abv": x[1], "ibu": x[2], "id": x[3], "name": x[4], "style": x[5], "brewery_id": x[6], "ounces": x[7]}) 
              | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
              | 'ChangeDataType' >> beam.Map(convert_types)
              | 'DeleteUnwantedData' >> beam.Map(del_unwanted_cols)
              | 'wiret' >> beam.io.WriteToText('gs://pipline-beam/batch/load_beers.csv')
        )

if __name__ == '__main__':
    run_pipeline()