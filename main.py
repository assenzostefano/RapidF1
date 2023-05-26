from dotenv import load_dotenv
from flask import Flask, jsonify
from fastf1.ergast import Ergast
import requests
import fastf1
import psycopg2
import os

fastf1.Cache.enable_cache('cache')  # optionally change cache location

app = Flask(__name__)

load_dotenv()

HOST_POSTGRESS = os.getenv('HOST_POSTGRESS')
PORT_POSTGRESS = os.getenv('PORT_POSTGRESS')
DB_POSTGRESS = os.getenv('DBNAME_POSTGRESS')
USER_POSTGRESS = os.getenv('USER_POSTGRESS')
PASSWORD_POSTGRESS = os.getenv('PASSWORD_POSTGRESS')

conn = psycopg2.connect(
    host=HOST_POSTGRESS,
    port=PORT_POSTGRESS,
    dbname=DB_POSTGRESS,
    user=USER_POSTGRESS,
    password=PASSWORD_POSTGRESS,
    sslmode='require'
)

@app.route('/api/driver/id/<driver_id>')
def get_circuit(driver_id):
    # Connettiti al database prendi le informazioni del pilota e mostrale in JSON
    conn = psycopg2.connect(
        host=HOST_POSTGRESS,
        port=PORT_POSTGRESS,
        database=DB_POSTGRESS,
        user=USER_POSTGRESS,
        password=PASSWORD_POSTGRESS,
        sslmode='require'
    )
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM drivers WHERE driver_id = '{driver_id}'")
    row = cur.fetchone()
    cur.close()

    if row:
        ID = row[0]
        driver_id = row[1]
        driver_name = row[2]
        driver_surname = row[3]
        driver_complete_name = row[4]
        driver_nationality = row[5]
        driver_url = row[6]
        driver_date_of_birth = row[7]
        driver_description = row[8]

        return_json = {
            'ID': ID,
            'Driver ID': driver_id,
            'Name': driver_name,
            'Surname': driver_surname,
            'Complete Name': driver_complete_name,
            'Nnationality': driver_nationality,
            'Url': driver_url,
            'Date of birth': driver_date_of_birth,
            'Description': driver_description
        }
        return jsonify(return_json)
    else:
        return "Driver not found"

@app.route('/api/driver/name/<driver_name>')
def get_driver_by_name(driver_name):
    # Connettiti al database prendi le informazioni del pilota e mostrale in JSON
    conn = psycopg2.connect(
        host=HOST_POSTGRESS,
        port=PORT_POSTGRESS,
        database=DB_POSTGRESS,
        user=USER_POSTGRESS,
        password=PASSWORD_POSTGRESS,
        sslmode='require'
    )
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM drivers WHERE given_name = '{driver_name}'")
    row = cur.fetchone()
    cur.close()

    if row:
        ID = row[0]
        driver_id = row[1]
        driver_name = row[2]
        driver_surname = row[3]
        driver_complete_name = row[4]
        driver_nationality = row[5]
        driver_url = row[6]
        driver_date_of_birth = row[7]
        driver_description = row[8]

        return_json = {
            'ID': ID,
            'Driver ID': driver_id,
            'Name': driver_name,
            'Surname': driver_surname,
            'Complete Name': driver_complete_name,
            'Nnationality': driver_nationality,
            'Url': driver_url,
            'Date of birth': driver_date_of_birth,
            'Description': driver_description
        }
        return jsonify(return_json)
    else:
        return "Driver not found"

@app.route('/api/driver/surname/<driver_surname>')
def get_driver_by_surname(driver_surname):
    # Connettiti al database prendi le informazioni del pilota e mostrale in JSON
    conn = psycopg2.connect(
        host=HOST_POSTGRESS,
        port=PORT_POSTGRESS,
        database=DB_POSTGRESS,
        user=USER_POSTGRESS,
        password=PASSWORD_POSTGRESS,
        sslmode='require'
    )
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM drivers WHERE family_name = '{driver_surname}'")
    row = cur.fetchone()
    cur.close()

    if row:
        ID = row[0]
        driver_id = row[1]
        driver_name = row[2]
        driver_surname = row[3]
        driver_complete_name = row[4]
        driver_nationality = row[5]
        driver_url = row[6]
        driver_date_of_birth = row[7]
        driver_description = row[8]

        return_json = {
            'ID': ID,
            'Driver ID': driver_id,
            'Name': driver_name,
            'Surname': driver_surname,
            'Complete Name': driver_complete_name,
            'Nnationality': driver_nationality,
            'Url': driver_url,
            'Date of birth': driver_date_of_birth,
            'Description': driver_description
        }
        return jsonify(return_json)
    else:
        return "Driver not found"

@app.route('/api/driver/complete_name/<driver_complete_name>')
def get_driver_by_complete_name(driver_complete_name):
    # Connettiti al database prendi le informazioni del pilota e mostrale in JSON
    conn = psycopg2.connect(
        host=HOST_POSTGRESS,
        port=PORT_POSTGRESS,
        database=DB_POSTGRESS,
        user=USER_POSTGRESS,
        password=PASSWORD_POSTGRESS,
        sslmode='require'
    )
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM drivers WHERE complete_name = '{driver_complete_name}'")
    row = cur.fetchone()
    cur.close()

    if row:
        ID = row[0]
        driver_id = row[1]
        driver_name = row[2]
        driver_surname = row[3]
        driver_complete_name = row[4]
        driver_nationality = row[5]
        driver_url = row[6]
        driver_date_of_birth = row[7]
        driver_description = row[8]

        return_json = {
            'ID': ID,
            'Driver ID': driver_id,
            'Name': driver_name,
            'Surname': driver_surname,
            'Complete Name': driver_complete_name,
            'Nnationality': driver_nationality,
            'Url': driver_url,
            'Date of birth': driver_date_of_birth,
            'Description': driver_description
        }
        return jsonify(return_json)
    else:
        return "Driver not found"
if __name__ == '__main__':
    app.run(debug=True)