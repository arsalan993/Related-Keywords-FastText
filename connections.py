from elasticsearch import Elasticsearch
from settings import (ELASTICSEARCH_HOSTS, ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)

try:
    # establishing connection with elasticsearch db
    elasticsearch_connection = Elasticsearch(hosts=ELASTICSEARCH_HOSTS,
                                             timeout=120,
                                             http_auth=(ELASTICSEARCH_USERNAME,
                                                        ELASTICSEARCH_PASSWORD),
                                             maxsize=1000,
                                             port=80
                                             )
except Exception as connection_failed_:
    print(connection_failed_)
