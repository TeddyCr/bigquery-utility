from google.cloud import bigquery
import pandas as pd
from hurry.filesize import size

class Query(object):
    """
    Abstract the querying process for GBQ tables.
    Object is instanciated by using Query(). The object has 2 methods:
        - standardQuery: returns an iterable composed of Row() functions
        - pandasQuery: returns a pandas DataFrame

    Each method takes 3 arguments:
        - query: the query to execute
        - legacy: determines if SQL language used is satandard or legacy. default to False (standard)
        - safe: determines if user should confirm the run before executing the query. If set to True, the area queried will be display.
    """
    def __init__(self):
        self.client = bigquery.Client()

    def standardQuery(self, query, legacy=False, location='US',safe=False):
        query = query
        if not safe:
            if not legacy:
                if bigquery.__version__ == '0.32.0':
                    query_job = self.client.query(query, location=location)
                else:
                    query_job = self.client.query(query)
                return query_job       
            else:
                query_config = bigquery.QueryJobConfig()
                query_config.use_legacy_sql = True
                if bigquery.__version__ == '0.32.0':
                    query_job = self.client.query(query, location=location, job_config=query_config)
                else:
                    query_job = self.client.query(query, job_config=query_config)                   
                return query_job
        else:
            answer = self.safeQuery(query, location, legacy)
            if not legacy:
                if answer[0].lower() == 'y':
                    if bigquery.__version__ == '0.32.0':
                        query_job = self.client.query(query, location=location)
                    else:
                        query_job = self.client.query(query)
                    return query_job
                elif answer[0].lower() == 'y':
                    return 'Query execution has been aborded by user'
                else:
                    raise ValueError('System could not understand user input. Make sure you\'ve entered "y" or "n"')
            else:
                query_conf = bigquery.QueryJobConfig()
                query_conf.use_legacy_sql = True

                if answer[0].lower() == 'y':
                    if bigquery.__version__ == '0.32.0':
                        query_job = self.client.query(query, location=location, job_config=query_conf)
                    else:
                        query_job = self.client.query(query, job_config=query_conf)
                    return query_job
                elif answer[0].lower() == 'y':
                    return 'Query execution has been aborded by user'
                else:
                    raise ValueError('System could not understand user input. Make sure you\'ve entered "y" or "n"') 

    def pandasQuery(self, query, legacy=False, location='US',safe=False):
        query = query

        if not safe:
            if not legacy:
                df = self.client.query(query).to_dataframe()
            else:
                query_conf = bigquery.QueryJobConfig()
                query_conf.use_legacy_sql = True

                df = self.client.query(query, job_config=query_conf).to_dataframe()
            return df
        else:
            answer = self.safeQuery(query, location,legacy)
            if answer[0].lower() == 'y':
                if not legacy:
                    df = self.client.query(query).to_dataframe()
                else:
                    query_config = bigquery.QueryJobConfig()
                    query_config.use_legacy_sql = True

                    df = self.client.query(query, job_config=query_config).to_dataframe()
                return df    
            elif answer[0].lower() == 'y':
                return 'Query execution has been aborded by user'
            else:
                raise ValueError('System could not understand user input. Make sure you\'ve entered "y" or "n"')             


    def safeQuery(self, query, location, legacy):
        query_config = bigquery.QueryJobConfig()
        query_config.dry_run = True
        query_config.use_query_cache = False
        if legacy:
           query_config.use_legacy_sql = True
        if bigquery.__version__ == '0.32.0':
            query_job = self.client.query(query, location=location, job_config=query_config)
        else:
            query_job = self.client.query(query, job_config=query_config)
        processed_size = size(query_job.total_bytes_processed)

        print(f'Your query will process {processed_size}, do you want to processed? [y/n]')
        answer = input()

        return answer
                
