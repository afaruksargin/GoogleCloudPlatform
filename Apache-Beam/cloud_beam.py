import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import pandas as pd



def convert_types(data):
  data['Kreditutar'] = float(data['Kreditutar']) if 'Kreditutar' in data else None
  data['yas'] = int(data['yas']) if 'yas' in data else None
  data['kredisayisi'] = int(data['kredisayisi']) if 'kredisayisi' in data else None
  return data

# Apache Beam ParDo işlevi
class PandasDataFrameDoFn(beam.DoFn):
    def process(self, element):
        # Veriyi Pandas DataFrame'e dönüştürelim
        df = pd.DataFrame([element])
        return [df]
    

def filter_dataframe(element, column_name, condition):
    # Pandas DataFrame'i oluşturmak için gerekli adımlar
    df = pd.DataFrame.from_dict(element)

    # Filtreleme işlemi
    filtered_df = df[df[column_name].apply(condition)]

    # Filtrelenmiş DataFrame'i Python sözlüğüne dönüştürme
    filtered_dict = filtered_df.to_dict(orient='records')

    return filtered_dict
def run_pipeline():
    schema = {
    'fields': [
        {'name': 'Kreditutar', 'type': 'FLOAT'},
        {'name': 'yas', 'type': 'FLOAT'},
        {'name': 'evdurmu', 'type': 'STRING'},
        {'name': 'kredisayisi', 'type': 'INTEGER'},
        {'name': 'tel', 'type': 'STRING'},
        {'name': 'cevap', 'type': 'STRING'}
    ]
}

    options = PipelineOptions([
        '--job_name=a',
        '--runner=DataflowRunner',
        '--project=jovial-evening-394610',
        '--region=us-central1',
        '--temp_location=gs://kredi-veri-seti/temp',
        '--staging_location=gs://kredi-veri-seti/staging'
    ])
    with beam.Pipeline(options=options) as pipeline:
        # CSV dosyasından veri okuma
        pcollection = (
            pipeline
            | 'Read CSV File' >> beam.io.ReadFromText('gs://kredi-veri-seti/batch/batch_krediVeriseti.csv', skip_header_lines=1)  # Başlıkları atlamak için skip_header_lines parametresi kullanılır
            | 'SplitData' >> beam.Map(lambda x: x.split(',')) # Her bir satır veriyi bir liste olarak tutmak için kullanılır
            | 'SplitData2' >> beam.Map(lambda x: x[0].split(';')) #Her bir liste içindeki değerin ; e göre ayrılarak liste içinde değişkenler olması için kullanılır
            | 'List to dict' >> beam.Map(lambda x : {'Kreditutar': x[0] , "yas": x[1], "evdurmu": x[2], "kredisayisi": x[3], "tel": x[4], "cevap": x[5]}) # listeyi sözlüke

        )
        convert_collection = (
            pcollection
            | 'convert_type'>> beam.Map(convert_types)
        )
        filter_collection = (
            convert_collection
            #| 'ConvertPandas' >> beam.ParDo(PandasDataFrameDoFn())
            #| 'Combine' >> beam.CombineGlobally(pd.concat)
            #| 'Filter DataFrame' >> beam.Map(filter_dataframe, column_name='Kreditutar', condition=lambda x: x > 5000)  # DataFrame'i filtreliyoruz
            | 'Wrtie' >> beam.io.Write(beam.io.WriteToBigQuery(
                table='deneme',
                dataset='beam',
                project='jovial-evening-394610',
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            ))
        )

if __name__ == '__main__':
    run_pipeline()