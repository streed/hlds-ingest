import base64
import json
import snappy

from flask import Flask, request

app = Flask(__name__)

@app.route("/", methods=['POST'])
def blob_ingest():
    blob = request.get_json()

    compressed_blob = blob['blob']

    base64ed = base64.b64decode(compressed_blob)

    logs = snappy.decompress(base64ed)

    print(json.loads(logs))

    return "test"
