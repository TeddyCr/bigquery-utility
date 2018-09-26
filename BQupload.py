from __future__ import print_function
from google.cloud import bigquery
from datetime import datetime, timedelta

class UploadGBQ(object):
    """
    Handles GBQ uploads. This class takes the following arguments:
        - file: the path to the file to upload to GBQ (currently handles only CSV upload) - STRING + REQUIRED
        - dataset_id: the dataset name where the table will be created - STRING + REQUIRED
        - table_id: the name of the table that will be created - STRING + REQUIRED
        - location: the location of the project default to 'US'- STRING
        - retention: set expiration date on the table in days
        - jagged_row: if true, it will handle unconsistent row lenght in table - BOOL
        - auto_detect: will assert tables schema based on data type in CSV - BOOL + REQUIRED IF SCHEMA EMPTY
        - schema: schema for the table is a list of tuple. Each tuple representing a field - TUPLE("name", "TYPE") + REQUIRED IF auto_detect=False
        - leading_row: number of rows to skip - INT
        - file_format: default to CSV for now - STRING 
        - write_disposition: write disposition for the table creation - STRING + 3 OPTIONS -> WRITE_EMPTY, WRITE_TRUNCATE, WRITE_APPEND
    """
    
    def __init__(self, file=None, dataset_id=None, table_id=None, location='US', partition_field=None, retention=32,jagged_row=None, auto_detect=False, schema=[], leading_row=0, file_format='CSV', write_disposition='WRITE_EMPTY'):
        if not file:
            raise ValueError("Error, file path is required")
        else:
            self.file = file
        if not dataset_id:
            raise ValueError("Error, dataset_id required")
        else:
            self.dataset_id = dataset_id
        if not table_id:
            raise ValueError("Error, table_id is required")
        else:
            self.table_id = table_id

        if not auto_detect and len(schema) == 0:
            raise ValueError('You must either provide a schema or set auto_detect=True')

        self.location = location
        self.partition_field = partition_field
        self.retention = retention
        self.jagged_row = jagged_row 
        self.auto_detect = auto_detect 
        self.schema = schema 
        self.leading_row = leading_row 
        self.file_format = file_format 
        self.write_disposition = write_disposition


    def uploadToGBQ_CSV(self):
        """
        Handles GBQ Upload for CSV files only
        """
        client = bigquery.Client() 

        dataset_ref = client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)
        filename = self.file

        job_config = bigquery.LoadJobConfig()
        if self.jagged_row:
            job_config.allow_jagged_rows = self.jagged_row
        if self.auto_detect:
            job_config.autodetect = self.auto_detect

        if len(self.schema) > 0 and self.auto_detect != True:
            sch = []
            for i in range(len(self.schema)):
                sch.append(bigquery.SchemaField(self.schema[i][0], self.schema[i][1]))
            job_config.schema = sch
        if self.leading_row > 0:
            job_config.skip_leading_rows = self.leading_row
        
        if self.partition_field:
            job_config._properties['load']['timePartitioning'] = {'type':'DAY', 'field': f'{self.partition_field}'}
        
        job_config.write_disposition = self.write_disposition

        ## location argument and bigquery.SourceFormat.CSV only available starting v0.32.0
        if float(bigquery.__version__[:-2]) >= 0.32:
            if self.file_format == 'CSV':
                job_config.source_format = bigquery.SourceFormat.CSV
            else:
                raise ValueError(f'Incorrect file format "{self.file_format}". Please enter "CSV" as the file format')

            try: 
                with open(filename, 'rb') as csv_source_file:
                    job = client.load_table_from_file(csv_source_file, table_ref, dataset_ref, location=self.location ,job_config=job_config)

                job.result()

                table = client.get_table(table_ref)
                expiration_date = datetime.now() + timedelta(days=self.retention)
                table.expires = expiration_date
                client.update_table(table, ['expires'])

                print(f"Upload Success. {job.output_rows} rows loaded at `{job.project}.{self.dataset_id}.{self.table_id}` ")
                return True
            except:
                raise
        
        else:
            try: 
                with open(filename, 'rb') as csv_source_file:
                    job = client.load_table_from_file(csv_source_file, table_ref, dataset_ref, job_config=job_config)
                
                job.result()

                table = client.get_table(table_ref)
                expiration_date = datetime.now() + timedelta(days=self.retention)
                table.expires = expiration_date
                client.update_table(table, ['expires'])

                print(f"Upload Success. {job.output_rows} rows loaded at `{job.project}.{self.dataset_id}.{self.table_id}` ")
                return True
            except:
                raise

    def uploadToGBQ_JSON(self):

        client = bigquery.Client()

        dataset_ref = client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)
        filename = self.file

        job_config = bigquery.LoadJobConfig()

        if self.partition_field:
            job_config._properties['load']['timePartitioning'] = {'type': 'DAY', 'field':f'{self.partition_field}'}

        if self.jagged_row:
            job_config.allow_jagged_rows = self.jagged_row

        if self.auto_detect:
            job_config.autodetect = self.auto_detect

        if len(self.schema) > 0 and self.auto_detect != True:
            sch = []
            for i in self.schema:
                sch.append(bigquery.SchemaField(self.schema[i][0],self.schema[i][1]))
            job_config.schema = sch
        
        if self.leading_row > 0:
            job_config.skip_leading_rows = self.leading_row

        job_config.write_disposition = self.write_disposition

        if float(bigquery.__version__[:-2]) >= 0.32:
            if self.file_format == 'JSON':
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            else:
                raise ValueError(f'Incorrect file format {self.file_format}. Please enter "JSON" as the file format')

            try:
                with open(filename, 'rb') as json_source_file:
                    job = client.load_table_from_file(json_source_file,table_ref, dataset_ref, location=self.location, job_config=job_config)

                job.result()

                table = client.get_table(table_ref)
                expiration_date = datetime.now() + timedelta(days=self.retention)
                table.expires = expiration_date
                client.update_table(table, ['expires'])

                print(f"Upload Success. {job.output_rows} rows loaded at `{job.project}.{self.dataset_id}.{self.table_id}` ")
                return True
            except:
                raise

        else:
            try:
                with open(filename, 'rb') as json_source_file:
                    job = client.load_table_from_file(json_source_file,table_ref, dataset_ref, job_config=job_config)

                job.result()

                table = client.get_table(table_ref)
                expiration_date = datetime.now() + timedelta(days=self.retention)
                table.expires = expiration_date
                client.update_table(table, ['expires'])

                print(f"Upload Success. {job.output_rows} rows loaded at `{job.project}.{self.dataset_id}.{self.table_id}` ")
                return True
            except:
                raise                    