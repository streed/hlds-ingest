import base64
import json
import snappy
import time

from flask import Flask, request
from elasticsearch import Elasticsearch

app = Flask(__name__)

class ElasticSearchSink(object):

    def __init__(self, host='127.0.0.1:9300'):
        self.es = Elasticsearch()

    def send(self, blobs):
        for blob in blobs:
            todaysIndex = '%s-%s' % (blob['type'], self.get_index())
            res = self.es.index(index=todaysIndex, doc_type=blob['type'], body=blob)
            print(res['_id'] + ': ' + repr(blob))

        return True


    def get_index(self):
        day = time.strftime("%d-%m-%Y")

        return 'hlds-%s' % day


def send_to_es(blobs):
    es = ElasticSearchSink()

    return es.send(blobs)


@app.route("/", methods=['POST'])
def blob_ingest():
    blob = request.get_json()

    compressed_blob = blob['blob']

    base64ed = base64.b64decode(compressed_blob)

    logs = snappy.decompress(base64ed)

    blobs = json.loads(logs)

    if send_to_es(blobs):
        return "sent"
    else:
        return "Error"
