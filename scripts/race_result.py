from dotenv import load_dotenv
import requests
import psycopg2
import datetime
import fastf1
import os

load_dotenv()

HOST_POSTGRESS = os.getenv('HOST_POSTGRESS')
PORT_POSTGRESS = os.getenv('PORT_POSTGRESS')
DB_POSTGRESS = os.getenv('DBNAME_POSTGRESS')
USER_POSTGRESS = os.getenv('USER_POSTGRESS')
PASSWORD_POSTGRESS = os.getenv('PASSWORD_POSTGRESS')

# Ottieni il percorso della cartella corrente
current_directory = os.path.dirname(os.path.abspath(__file__))

# Costruisci il percorso della cartella di cache relativo al percorso corrente
cache_directory = os.path.join(current_directory, '..', 'cache')

fastf1.Cache.enable_cache(cache_directory)  # optionally change cache location

# Funzione per salvare i risultati delle gare per un dato anno
def save_race_results():
    conn = psycopg2.connect(
        host=HOST_POSTGRESS,
        port=PORT_POSTGRESS,
        database=DB_POSTGRESS,
        user=USER_POSTGRESS,
        password=PASSWORD_POSTGRESS,
        sslmode='require'
    )
    cur = conn.cursor()
    # Take current year
    current_year = datetime.datetime.now().year
    current_year += 1
    years = range(1950, current_year)
    start_year_fastf1 = 2018
    for year in years:
        print(year)
        url = f"http://ergast.com/api/f1/{year}/results.json"
        if year >= start_year_fastf1 and year <= current_year:
            # Take all races in a year
            max_race_number = 50
            for i in range(1, max_race_number):
                try:
                    session = fastf1.get_session(year, i , 'R')
                    session.load()
                    # Get the race name
                    circuit_name = session.name
                    # Get the race id
                    circuit_id = session.circuit_id
                    
                except:
                    break
        else:
            response = requests.get(url)
            data = response.json()
            races = data['MRData']['RaceTable']['Races']
            for race in races:
                season = float(race['season'])
                print(season)
                circuit_name = race['raceName']
                circuit_id = race['Circuit']['circuitId']
                date = race['date']
                time = race.get('time', '')
                lat = race['Circuit']['Location']['lat']
                long = race['Circuit']['Location']['long']
                locality = race['Circuit']['Location']['locality']
                country = race['Circuit']['Location']['country']
                for result in race['Results']:
                    number = result['number']
                    position = result['position']
                    position_text = result['positionText']
                    points = result['points']
                    driver_id = result['Driver']['driverId']
                    driver_name = result['Driver']['givenName'] + ' ' + result['Driver']['familyName']
                    driver_nationality = result['Driver']['nationality']
                    constructor_id = result['Constructor']['constructorId']
                    constructor_name = result['Constructor']['name']
                    constructor_nationality = result['Constructor']['nationality']
                    constructor_grid = result['grid']
                    constructor_laps = result['laps']
                    constructor_status = result['status']
                    time = result.get('Time', {}).get('time', '')
                    fastest_lap_rank = result.get('FastestLap', {}).get('rank', '')
                    fastest_lap_lap = result.get('FastestLap', {}).get('lap', '')
                    try:
                        average_speed_units = result['FastestLap']['AverageSpeed']['units']
                    except:
                        average_speed_units = ''
                    try:
                        average_speed_speed = result['FastestLap']['AverageSpeed']['speed']
                    except:
                        average_speed_speed = ''

                    print(f"Season: {season}")
                    print(f"Circuit: {circuit_name} ({circuit_id})")
                    print(f"Date: {date} Time: {time}")
                    print(f"Location: {locality}, {country} (Lat: {lat}, Long: {long})")
                    print(f"Number: {number}")
                    print(f"Position: {position} ({position_text})")
                    print(f"Points: {points}")
                    print(f"Driver: {driver_name} ({driver_id}), Nationality: {driver_nationality}")
                    print(f"Constructor: {constructor_name} ({constructor_id}), Nationality: {constructor_nationality}")
                    print(f"Grid: {constructor_grid}")
                    print(f"Laps: {constructor_laps}")
                    print(f"Status: {constructor_status}")
                    print(f"Time: {time}")
                    print(f"Fastest Lap: Rank: {fastest_lap_rank}, Lap: {fastest_lap_lap}")
                    print(f"Average Speed: {average_speed_speed} {average_speed_units}")
                    print("------------------------")

    cur.close()
    conn.close()

save_race_results()