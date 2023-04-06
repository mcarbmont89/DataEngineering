import requests
import base64
import json
import psycopg2

# credenciales de la API de Spotify
CLIENT_ID = '3f58de94bd1745389891a36627a1cc62'
CLIENT_SECRET = 'aff2cacecd68459ea257420ebceb0be2'

# credenciales de Redshift
REDSHIFT_HOST = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
REDSHIFT_PORT = '5439'
REDSHIFT_USER = 'mcarbmont89_coderhouse'
REDSHIFT_PASS = 'vSPJ5L68rA6C'
REDSHIFT_DB = 'data-engineer-database'


def get_access_token(client_id, client_secret):
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_data = {
        'grant_type': 'client_credentials'
    }
    auth_header = {
        'Authorization': 'Basic ' + base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    }
    
    response = requests.post(auth_url, data=auth_data, headers=auth_header)
    if response.status_code == 200:
        access_token = response.json()['access_token']
        return access_token
    else:
        return False


def search_spotify_songs(access_token, query):
    search_url = 'https://api.spotify.com/v1/search'
    search_params = {
        'q': query,
        'type': 'track',
        'limit': 10
    }
    search_header = {
        'Authorization': f"Bearer {access_token}"
    }
    
    response = requests.get(search_url, params=search_params, headers=search_header)
    if response.status_code == 200:
        return response.json()
    else:
        return False


def create_redshift_table(conn):
    cursor = conn.cursor()
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS fito_paez_songs (
        id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(200),
        artist_name VARCHAR(200),
        album_name VARCHAR(200)
    );
    '''

    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()


def insert_songs_into_redshift(conn, songs):
    cursor = conn.cursor()

    for song in songs:
        insert_query = f"INSERT INTO fito_paez_songs (id, name, artist_name, album_name) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;"
        cursor.execute(insert_query, (song['id'], song['name'], song['artist_name'], song['album_name']))

    conn.commit()
    cursor.close()


def main():
    access_token = get_access_token(CLIENT_ID, CLIENT_SECRET)
    search_query = "Fito Páez"
    data = search_spotify_songs(access_token, search_query)

    # Imprimir datos 
    print(json.dumps(data, indent=2))

    # Extraer e imprimir información de las canciones
    tracks = data['tracks']['items']
    print("\nCanciones de Fito Páez:")
    for track in tracks:
        print(f"{track['name']} - {track['artists'][0]['name']} - {track['album']['name']}")

    # Extraer información de las canciones
    tracks = data['tracks']['items']
    songs = []
    for track in tracks:
        song = {
            'id': track['id'],
            'name': track['name'],
            'artist_name': track['artists'][0]['name'],
            'album_name': track['album']['name']
        }
        songs.append(song)


    # Conectar a Redshift
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASS
    )

    # Crear la tabla en Redshift
    create_redshift_table(conn)

    # Insertar canciones en la tabla de Redshift
    insert_songs_into_redshift(conn, songs)
