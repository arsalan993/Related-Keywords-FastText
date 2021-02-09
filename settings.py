from decouple import config
import json

# API_URL = config("API_URL", default='https://api-knowledge-graph-search.contentstudio.io')
ELASTICSEARCH_HOSTS = json.loads(config("ELASTICSEARCH_HOSTS"))
ELASTICSEARCH_USERNAME = config("ELASTICSEARCH_USERNAME")
ELASTICSEARCH_PASSWORD = config("ELASTICSEARCH_PASSWORD")