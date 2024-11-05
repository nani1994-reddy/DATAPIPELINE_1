import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import ReadFromText
import json

class FormatData(beam.DoFn):
    def process(self, element):
        from datetime import datetime, timezone

        data = json.loads(element)
        data['company_name'] = data['company_name'].strip().upper()  # Ensuring consistent format
        data['country'] = data['country'].strip().upper()
        data['industry'] = " ".join(word.capitalize() for word in data['industry'].split())
        try:
            size_range = data.get('size', '0').split('-')
            data['size'] = max(map(int, size_range))
        except ValueError:
            data['size'] = 0

        data['load_date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        headquarters = data.get('headquarters', '').split(',')
        data['city'] = headquarters[0].strip() if len(headquarters) > 0 else None
        data['state'] = headquarters[1].strip() if len(headquarters) > 1 else None

         # Set default value for latest_news if it's missing or empty
        data['latest_news'] = data.get('latest_news') or 'Not available'


        data.pop('description', None)
        data.pop('locationUrl', None)
        data.pop('headquarters', None)

        yield data

class LoadCEOData(beam.DoFn):
    def process(self, element):
        # Assuming CSV format: "company_name,ceo"
        fields = element.split(',')
        if len(fields) == 2:
            company_name, ceo = fields
            yield (company_name.strip().upper(), ceo.strip())

def run():
    pipeline_options = PipelineOptions()
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'project1-439615'
    google_cloud_options.region = 'us-east1'
    google_cloud_options.staging_location = 'gs://company_ceo/staging'
    google_cloud_options.temp_location = 'gs://company_ceo/temp'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    table_schema = {
        'fields': [
            {'name': 'company_name', 'type': 'STRING'},
            {'name': 'location', 'type': 'STRING'},
            {'name': 'country', 'type': 'STRING'},
            {'name': 'industry', 'type': 'STRING'},
            {'name': 'website', 'type': 'STRING'},
            {'name': 'size', 'type': 'INTEGER'},
            {'name': 'ceo', 'type': 'STRING'},
            {'name': 'latest_news', 'type': 'STRING'},
            {'name': 'linkedin_url', 'type': 'STRING'},
            {'name': 'point_of_contact', 'type': 'STRING'},
            {'name': 'id', 'type': 'STRING'},
            {'name': 'specialties', 'type': 'STRING'},
            {'name': 'founded', 'type': 'STRING'},
            {'name': 'load_date', 'type': 'DATE'},
            {'name': 'state', 'type': 'STRING'},
            {'name': 'city', 'type': 'STRING'},
        ]
    }

    with beam.Pipeline(options=pipeline_options) as pipeline:
        main_data = (
            pipeline
            | 'Read Main Data' >> ReadFromText('gs://company_ceo/sample_suppliers_data.json')
            | 'Format Main Data' >> beam.ParDo(FormatData())
            | 'Map Main Data by Company' >> beam.Map(lambda row: (row['company_name'], row))
        )

        ceo_data = (
            pipeline
            | 'Read CEO Data' >> ReadFromText('gs://company_ceo/company_ceo_data.csv')
            | 'Parse CEO Data' >> beam.ParDo(LoadCEOData())
        )

        # Log data before merging to verify
        main_data | 'Log Main Data' >> beam.Map(lambda x: logging.info(f"Main Data: {x}"))
        ceo_data | 'Log CEO Data' >> beam.Map(lambda x: logging.info(f"CEO Data: {x}"))

        # Merging data by company_name
        merged_data = (
            {'main': main_data, 'ceo': ceo_data}
            | 'Join Main and CEO Data' >> beam.CoGroupByKey()
            | 'Merge CEO to Main Data' >> beam.Map(lambda company: {
                  **company[1]['main'][0],
                  'ceo': company[1]['ceo'][0] if company[1]['ceo'] else 'Not available'
              } if company[1]['main'] else {})
        )

        # Log merged data to confirm CEO inclusion
        merged_data | 'Log Merged Data' >> beam.Map(lambda x: logging.info(f"Merged Data: {x}"))

        merged_data | 'Write to BigQuery' >> WriteToBigQuery(
            table='project1-439615.ceo_dataset.company_list',
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
