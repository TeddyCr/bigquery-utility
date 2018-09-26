from google.cloud import bigquery

def isLocationArgVersion():
    if float(bigquery.__version__[:-2]) >= 0.32:
        return True
    else:
        return False