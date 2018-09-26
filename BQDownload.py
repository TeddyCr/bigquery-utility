from __future__ import print_function
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, timedelta
from bigQueryUtility import helpers
import os

class DownloadGBQ(object):

    def __init__(self):
        """
        Initialize Download class
        """

    def GBQTableToGCS(self, **kwargs):
        self.bucket = kwargs.get('bucket', None)
        self.destination = kwargs.get('destination', None)
        self.dataset = kwargs.get('dataset', None)
        self.table = kwargs.get('table', None)
        self.field_delimiter = kwargs.get('field_delimiter', ',')

        client = bigquery.Client()

        self.destination_uri = f'gs://{self.bucket}/{self.destination}'

        self.dataset_ref = client.dataset(self.dataset)
        self.table_ref = self.dataset_ref.table(self.table)

        job_config = bigquery.ExtractJobConfig()
        job_config.field_delimiter = self.field_delimiter

        if helpers.isLocationArgVersion():
            extract_job = client.extract_table(self.table_ref, self.destination_uri, location='US', job_config=job_config)
            extract_job.result()
        else:
            extract_job = client.extract_table(self.table_ref, self.destination_uri,job_config=job_config)
            extract_job.result()

        return extract_job
    
    def exportGCSFile(self, **kwargs):
        self.bucket = kwargs.get('bucket', None)
        self.destination = kwargs.get('destination', None)
        self.filename = kwargs.get('filename', None)
        self.prefix = kwargs.get('prefix', None)
        self.delete = kwargs.get('delete', False)

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.bucket)

        if not self.prefix:
            blob = bucket.blob(self.destination)
            blob.download_to_filename(self.filename)

        else:
            blobs = bucket.list_blobs(prefix=self.prefix)
            for blob in blobs:
                blob_name = blob.name
                i = blob_name.rfind('/')
                if len(blob_name[i+1:]) > 0:
                    destination_file = os.path.join(self.filename,blob_name[i+1:])

                    blob.download_to_filename(destination_file)
                    if self.delete:
                        blob.delete()

        return True




        
        
        