import csv
import os
import sqlite3
from datetime import datetime
from io import StringIO

from flask import (Flask, request, jsonify, logging, g)

from helpers import get_upload_folder

WORKING_DIR = os.path.dirname(os.path.realpath(__file__))

DB_NAME = '3piano.db'
DATABASE = os.path.join(WORKING_DIR, DB_NAME)

app = Flask(__name__)
logger = logging.create_logger(app)


class DBOperations:

    cur = None

    def __init__(self):
        self.table_name = '3piano_cities'
        self.db = None
        self.cur = None
        self._db_connector()

    def _db_connector(self, close=False):
        if close:
            if self.db is not None:
                self.db.close()
        else:
            self.db = getattr(g, '_database', None)
            if self.db is None:
                self.db = g._database = sqlite3.connect(DATABASE)

    def insert_db(self, query, *args):
        self.cur = self.db.cursor()
        self.cur.execute(query % {'table_name': self.table_name}, args)
        self.cur.close()

    def query_db(self, query, *args, one=False):
        self.cur = self.db.cursor()
        self.cur.execute(query % {'table_name': self.table_name}, args)
        rv = self.cur.fetchall()
        self.cur.close()
        return (rv[0] if rv else None) if one else rv


dbo = DBOperations()


def write_file_to_db(file):
    with open(file, 'r') as _file:
        lines = csv.reader(_file, delimiter=";")

        query = "INSERT INTO %(table_name)s (city_name, lat, lon, country_name, country_code) values (?, ?, ?, ? ,?)"
        for line in lines:
            dbo.insert_db(query, *line)


@app.route('/upload/', methods=['POST'])
def upload():
    success = False
    result_text = ''
    if request.method == 'POST':
        try:
            file = request.files['file']
            file_name, file_ext = os.path.splitext(file.filename)

            if file and file_ext.lower() == '.csv':
                file_uploaded_as = os.path.join(get_upload_folder(WORKING_DIR), file.filename)
                file.save(file_uploaded_as)
                write_file_to_db(file_uploaded_as)
                success = True
            else:
                raise OSError('File not received or not a CSV file')

        except Exception as uee:
            result_text = uee
            print(uee.args, type(uee))
            logger.error(str(datetime.now()) + ' ' + str(result_text))

    return jsonify({
        'status': success,
        'junk_data': [],
        'message': 'result_text'
    })


@app.route('/')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
