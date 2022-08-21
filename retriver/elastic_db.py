import logging
import os

from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(" Establishing Elasticsearch Database ... ")

class ElasticDB(object):

    def __init__(self, elastic_port=None, elastic_index=None, elastic_fields=None, elastic_doc=None):
        """
        Args:
            :param elastic_port: Port for Elasticsearch instance
            :param elastic_index: Index name
            :param elastic_fields: Search fields
        """
        # Connect
        self.elastic_port = elastic_port
        logger.info('Connecting to %s' % elastic_port)

        self.es = Elasticsearch(elastic_port)
        self.elastic_index = elastic_index
        self.elastic_fields = elastic_fields
        self.elastic_doc = elastic_doc

    def add_doc(self, doc):
        """ Add doc to DB """
        response = self.es.index(
            index = self.elastic_index,
            document = doc,
        )

        logger.info('Added document id %s' % response["_id"])

    def search(self, query, k=5):
        results = self.es.search(
            index=self.elastic_index,
            body= {
                "size": k,
                "query": {
                    "match": {
                        "text": query,
            }}})

        hits = results["hits"]["hits"]
        doc_ids = [row['_source']["id"] for row in hits]

        return (hits, doc_ids)

    # Elasticsearch Controls

    def __enter__(self):
        return self

    def __close__(self):
        self.es = None

    # def get_doc_index(self, doc_id, field_doc_name):
    #     """Convert doc_id --> doc_index"""
    #     field_index = field_doc_name
    #     if isinstance(field_index, list):
    #         field_index = '.'.join(field_index)
    #     result = self.es.search(index=self.elastic_index, body={'query':{'match':
    #         {field_index: doc_id}}})
    #     return result['hits']['hits'][0]['_id']

    # def get_doc_text(self, doc_id):
    #     """ Fetch doc text for given doc_id """
    #     idx = self.get_doc_index(doc_id)



