import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Cloud Storage'da bulunan IRIS veri kümesinin yolunu ve hedef yolunu belirtin
INPUT_BUCKET = 'gs://data-iris/IRIS.csv'
OUTPUT_BUCKET = 'gs://data-iris/iris_output.csv'

def process_data(data):
    # Verileri işleyen fonksiyon
    return data.upper()

def run_pipeline():
    # Apache Beam PipelineOptions oluşturma
    options = PipelineOptions([
        '--job_name=dataflow_demo'
        '--runner=DataflowRunner',  # DataflowRunner kullanarak çalıştırmak için
        '--project=jovial-evening-394610',  # Proje adınızı belirtin
        '--region=us-central1',  # Dataflow işlerini başlatmak için coğrafi bölge
        '--temp_location=gs://data-iris/tmp',  # Geçici verilerin depolanacağı Cloud Storage yolunu belirtin
        '--staging_location=gs://data-iris/staging',  # İş akışı kodunun yükleneceği Cloud Storage yolunu belirtin
    ])

    # Pipeline oluşturma
    with beam.Pipeline(options=options) as p:
        # Cloud Storage'dan IRIS veri kümesini PCollection olarak okuma
        data = p | beam.io.ReadFromText(INPUT_BUCKET)

        # Verileri işleyin
        processed_data = data | beam.Map(process_data)

        # İşlenmiş verileri Cloud Storage'a yazma
        processed_data | beam.io.WriteToText(OUTPUT_BUCKET)

if __name__ == '__main__':
    run_pipeline()