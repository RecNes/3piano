import csv
import os
import sqlite3

from flask import (Flask, request, jsonify)

from helpers import get_upload_folder

WORKING_DIR = os.path.dirname(os.path.realpath(__file__))

DB_NAME = '3piano.db'
DATABASE = os.path.join(WORKING_DIR, DB_NAME)

app = Flask(__name__)


def write_file_to_db(file):
    try:
        with open(file, 'r') as _file:
            lines = csv.reader(_file, delimiter=";")

            with sqlite3.connect(DB_NAME) as con:
                cur = con.cursor()
                for count, line in enumerate(lines):
                    if count < 2:
                        continue
                    line_args = [x.strip('"') for x in line[0].split(',"')]
                    cur.execute(
                        "INSERT INTO '3piano_cities' ('city_name', 'lat', 'lon', 'country_name', 'country_code') VALUES (?, ?, ?, ? ,?)",
                        line_args)
                con.commit()
        return "Data transferred to database"
    except Exception as uee:
        return uee


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
                result_text = write_file_to_db(file_uploaded_as)
                success = True
            else:
                raise OSError('File not received or not a CSV file')

        except Exception as uee:
            result_text = uee
            print(uee.args, type(uee))

    return jsonify({
        'status': success,
        'message': result_text
    })


@app.route('/fetch/<country_code>/')
@app.route('/fetch/')
def fetch_city(country_code=None):
    success = False
    rows = []
    if country_code is None or not country_code:
        return jsonify({
            'status': success,
            'data': rows,
            'message': 'You have to specify country code; Eg. http://example.com/fetch/us/'
        })

    with sqlite3.connect(DB_NAME) as con:
        cur = con.cursor()
        cur.execute(
            "SELECT city_name FROM '3piano_cities' WHERE country_code=?", (country_code.lower(), ))
        rows = cur.fetchall()

    data = []
    for row in rows:
        data.append(row[0])
    success = True
    return jsonify({
            'status': success,
            'data': data,
            'message': ''
        })


if __name__ == '__main__':
    app.run()
