from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
import logging

import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials # To authenticate
import lyricsgenius

log = logging.getLogger(__name__)

# =============================================================================
# 1. Set up the main configurations of the dag
# =============================================================================


default_args = {
    'start_date': datetime(2021, 3, 10),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'bucket_name': 'ucl-msin0166-2021-spotify-project',
    'prefix': 'test_folder',
    'db_name': Variable.get("spotify_db_name", deserialize_json=True)['db_name'],
    # 'spotify_client_id': Variable.get("spotify_client_id", deserialize_json=True)['client_id_sp'],
    # 'spotify_client_secret': Variable.get("spotify_client_secret", deserialize_json=True),
    # 'spotify_genius_cred': Variable.get("spotify_genius_cred", deserialize_json=True)['genius_cred'],
    'aws_conn_id': "spotify_aws_default",
    'postgres_conn_id': 'spotify_postgres_conn_id',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('spotify',
    description='spotify',
    schedule_interval='@weekly',
    catchup=False,
    default_args=default_args,
    max_active_runs=1)

# =============================================================================
# 2. Define different functions
# =============================================================================
def create_rest_tables_in_db(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    log.info('Initialised connection')

    query = """ 
        CREATE SCHEMA IF NOT EXISTS spotify;

        DROP TABLE IF EXISTS spotify.artists;
        CREATE TABLE spotify.artists (
            artist_id     varchar(256),
            artist_name   varchar(256),
            followers     varchar(256),
            popularity    varchar(256)
        );

        DROP TABLE IF EXISTS spotify.albums;
        CREATE TABLE IF NOT EXISTS spotify.albums (
            album_id       varchar(256),
            artist_id      varchar(256),
            album_group    varchar(256),
            album_type     varchar(256),
            album_name     varchar(256),
            release_date   date,
            total_tracks   int
        );

        DROP TABLE IF EXISTS spotify.albums_images_links;
        CREATE TABLE IF NOT EXISTS spotify.albums_images_links (
            album_id     varchar(256),
            image_number int,
            height       int,
            width        int,
            url          varchar(256)
        );

        DROP TABLE IF EXISTS spotify.tracks;
        CREATE TABLE IF NOT EXISTS spotify.tracks (
            track_id     varchar(256),
            album_id     varchar(256),
            disc_number  int,
            duration_ms  numeric,
            explicit     boolean,
            is_local     boolean,
            track_name   varchar(256),
            preview_url  varchar(256),
            track_number int,
            track_type   varchar(256)
        );

        DROP TABLE IF EXISTS spotify.audio_features;
        CREATE TABLE IF NOT EXISTS spotify.audio_features (
            track_id        varchar(256),
            danceability    numeric,
            energy          numeric,
            key             int,
            loudness        numeric,
            mode            int,
            speechiness     numeric,
            acousticness    numeric,
            intrumentalness numeric,
            liveness        numeric,
            valence         numeric,
            tempo           numeric,
            register_type   varchar(256),
            duration_ms     numeric,
            time_signature  int
        );

        DROP TABLE IF EXISTS spotify.lyrics;
        CREATE TABLE IF NOT EXISTS spotify.lyrics (
        artist_name varchar(256),
        track_name  varchar(256),
        track_id    varchar(256),
        lyrics      varchar(10485760)
        );
    """

    cursor.execute(query)
    conn.commit()
    log.info("Created Schema and Tables")

def insert_rows_artists(**kwargs):
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials

    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info("Established connection to S3 bucket")

    bucket_name = kwargs['bucket_name']
    key = Variable.get("spotify_get_csv_artists", deserialize_json=True)
    key = key['key']

    paths = s3.list_keys(bucket_name=bucket_name, prefix="artist_names/")
    log.info("{}".format(paths))

    log.info("Reading CSV File artist names")
    artist_names = s3.read_key(key, bucket_name)
    artist_names = artist_names.split('\r\n')[1:-1]

    log.info("Read artists names")
    log.info("{}".format(artist_names))

    client_id =  Variable.get("spotify_client_id", deserialize_json=True)['client_id_sp']
    log.info("Got client_id")
    client_secret =  Variable.get("spotify_client_secret", deserialize_json=True)['key']
    log.info("Got client secret")

    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.client.Spotify(client_credentials_manager=client_credentials_manager)

    log.info("Established connection to sp")

    def get_artists(query):
        '''function that extracts all artists from spotify with a similar name to arguemnt "query"'''

        artists_list=[]
    
        results=sp.search(q=query,type='artist') # json format 
        ar=results['artists'] 
    
        items=ar['items']#contains the list of all artist that approximately match the query, then we iterate across it
        for it in items:
            if it['name'] == query:
                artist_id = it['id']
                artist_name = it['name']
                followers = it['followers']['total']
                popularity = it['popularity']
                artist = [artist_id,artist_name,followers,popularity]
                artists_list.append(artist)
                
        return artists_list

    log.info("Running queries in sp API")
    artists = []#a list of all artists extracted from spotify
    
    for query in artist_names:
        result_artists = get_artists(query)

        artists = artists + result_artists
    
    log.info('Ran queries in sp API')
    log.info('{}'.format(artists))

    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    s = """INSERT INTO spotify.artists (artist_id, artist_name, followers, popularity) VALUES (%s, %s, %s, %s)"""
    log.info('Updating rows in artists table')

    cursor.executemany(s, artists)
    conn.commit()
    log.info('Updated rows in artists table')

    conn.close()

def insert_rows_albums(**kwargs):
    import pandas as pd
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials

    client_id =  Variable.get("spotify_client_id", deserialize_json=True)['client_id_sp']
    log.info("Got client_id")
    client_secret =  Variable.get("spotify_client_secret", deserialize_json=True)['key']
    log.info("Got client secret")

    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    log.info("Established connection to sp")

    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # select artist_id from artists table

    q = "SELECT artist_id FROM spotify.artists;"
    cursor.execute(q)
    conn.commit()
    artist_ids = cursor.fetchall()

    # get albums
    def get_albums(artist):
        #a function that extracts from spotify all albums of a given artist
        artist_albums=[]
        results=sp.artist_albums(artist)
        items=results['items']#contains a list of all the artist albums, then we iterate across it 
        for it in items:
            album_id=it['id']
            album_group=it['album_group']
            album_type=it['album_type']
            album_name=it['name']
            release_date=it['release_date']
            total_tracks=it['total_tracks']
            artist_id=artist
            album=[album_id,artist_id,album_group,album_type,album_name,release_date,total_tracks]
            artist_albums.append(album)
        return artist_albums

    #extract all albums of all artist we got from spotify
    log.info("Running queries in sp API")
    albums_list=[]
    map_artist_ids = map(list, artist_ids)

    for artist in map_artist_ids:
        artist_albums=get_albums(artist[0])
        albums_list=albums_list+artist_albums
    log.info('Ran queries in sp API')
 
    albums_list = pd.DataFrame(albums_list)
    albums_list.iloc[:, -2] = albums_list.iloc[:, -2].apply(lambda x: x + '-01-01' if len(x) == 4 else(x + '-01' if len(x) == 7 else x))
    albums_list.iloc[:, -2] = albums_list.iloc[:, -2].apply(lambda x: '1996-09-23' if len(x) > 10 else x)

    #insert data from list
    s = """INSERT INTO spotify.albums(album_id,artist_id,album_group,album_type,album_name,release_date,total_tracks) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    log.info('Updating rows in albums table')

    cursor.executemany(s, list(albums_list.values))
    conn.commit()
    log.info('Updated rows in albums table')

    conn.close()

def insert_rows_tracks(**kwargs):
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials

    client_id =  Variable.get("spotify_client_id", deserialize_json=True)['client_id_sp']
    log.info("Got client_id")
    client_secret =  Variable.get("spotify_client_secret", deserialize_json=True)['key']
    log.info("Got client secret")

    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    log.info("Established connection to sp")

    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    log.info("Got connection to postgres")

    # select artist_id from artists table

    q = "SELECT album_id FROM spotify.albums;"
    cursor.execute(q)
    conn.commit()
    album_ids = cursor.fetchall()
    log.info("Got album_ids")

    # get tracks
    def get_tracks(album):
        #a function that extracts from spotify all tracks of a given album
        album_tracks=[]
        results=sp.album_tracks(album)
        items=results['items']#contains a list of all the album tracks, then we iterate across it 
        for it in items:
            track_id=it['id']
            disc_number=it['disc_number']
            duration_ms=it['duration_ms']
            explicit=it['explicit']
            is_local=it['is_local']
            track_name=it['name']
            preview_url=it['preview_url']
            track_number=it['track_number']
            track_type=it['type']
            album_id=album
            track=[track_id,album_id,disc_number,duration_ms,explicit,is_local,track_name,preview_url,track_number,track_type]
            album_tracks.append(track)
        return album_tracks


    #extract all tracks of all albums we got from spotify
    album_ids = map(list, album_ids)

    s = """INSERT INTO spotify.tracks(track_id,album_id,disc_number,duration_ms,explicit,is_local,track_name,preview_url,track_number,track_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    log.info("Getting album tracks")
    log.info("Getting album tracks and inserting to table")
    for album in album_ids:
        tracks_list=[]

        album_tracks=get_tracks(album[0])

        tracks_list=tracks_list+album_tracks

        cursor.executemany(s, list(tracks_list))
        conn.commit()

    conn.close()

def insert_rows_audio_features(**kwargs):
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials

    client_id =  Variable.get("spotify_client_id", deserialize_json=True)['client_id_sp']
    log.info("Got client_id")
    client_secret =  Variable.get("spotify_client_secret", deserialize_json=True)['key']
    log.info("Got client secret")

    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    log.info("Established connection to sp")

    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    q = "SELECT track_id FROM spotify.tracks;"
    cursor.execute(q)
    conn.commit()
    track_ids = cursor.fetchall()
    log.info("Got track ids")

    # get audio features
    def get_audio_features(track):

        audio_features=[]
        results=sp.audio_features([track])
        re=results.pop()

        audio_features_id=re['id']
        danceability=re['danceability']
        energy=re['energy']
        key=re['key'] 
        loudness=re['loudness']
        mode=re['mode']
        speechiness=re['speechiness']
        acousticness=re['acousticness']
        instrumentalness=re['instrumentalness']
        liveness=re['liveness']
        valence=re['valence']
        tempo=re['tempo']
        register_type=re['type']
        duration_ms=re['duration_ms']
        time_signature=re['time_signature']
        
        features=[audio_features_id,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,register_type,duration_ms,time_signature]
        audio_features.append(features)
        return audio_features


    #extract all audio features of all tracks we got from spotify
    map_track_ids = map(list, track_ids)

    s = """INSERT INTO spotify.audio_features(track_id,danceability,energy,key,loudness,mode,speechiness,acousticness,intrumentalness,liveness,valence,tempo,register_type,duration_ms,time_signature) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    log.info("Getting track features")
    for track in map_track_ids:
        audio_features_list=[]

        try:
            audio_features=get_audio_features(track[0])
        except:
            continue
        audio_features_list=audio_features_list+list(audio_features)

        cursor.executemany(s, list(audio_features))
        conn.commit()        

    log.info("Ran query")

    conn.close()

def insert_rows_image_links(**kwargs):
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials

    client_id =  Variable.get("spotify_client_id", deserialize_json=True)['client_id_sp']
    log.info("Got client_id")
    client_secret =  Variable.get("spotify_client_secret", deserialize_json=True)['key']
    log.info("Got client secret")

    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    log.info("Established connection to sp")

    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    q = "SELECT album_id FROM spotify.albums;"
    cursor.execute(q)
    conn.commit()
    album_ids = cursor.fetchall()
    log.info("Got album_ids")

    # get image links
    def get_images_links(album):
        
        images_links=[]
        results=sp.album(album)
        images=results['images']
        
        image_number=1
        for im in images:
            height=im['height']
            url=im['url']
            width=im['width']
            album_id=album
            image=[album_id,image_number,height,width,url]
            images_links.append(image)
            image_number=image_number+1
        return images_links

    #extract images of all albums we got from spotify
    album_ids = map(list, album_ids)

    s = """INSERT INTO spotify.albums_images_links(album_id,image_number,height,width,url) VALUES (%s, %s, %s, %s, %s)"""

    log.info("Getting image linkes")
    for album_id in album_ids:
        images_links_list=[]
        images_links=get_images_links(album_id[0])

        images_links_list=images_links_list+images_links

        cursor.executemany(s, images_links)
        conn.commit()

    log.info("Run query")

    conn.close()

def insert_rows_lyrics(**kwargs):
    import lyricsgenius

    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    q = """
    with artist_album_id as (
        select 
            a.artist_name,
            b.album_id
        from spotify.artists a
        left join spotify.albums b 
        on a.artist_id = b.artist_id
        ), track_album_id as (
        select
            a.track_name,
            a.track_id,
            b.album_id
        from spotify.tracks a 
        left join spotify.albums b 
        on a.album_id = b.album_id 
        ), artist_track as (
        select
            a.artist_name,
            b.track_name,
            b.track_id
        from artist_album_id a 
        inner join track_album_id b 
        on a.album_id = b.album_id
        )

        select 
            a.artist_name, 
            a.track_name,
            a.track_id
        from artist_track a"""

    cursor.execute(q)
    conn.commit()
    artist_tracks = cursor.fetchall()
    log.info("Got artist name, track name, track id")

    class lyrics:
        def __init__(self, artist, song=None, identifier_type = "artist"):
            self.geniusCreds = Variable.get("spotify_genius_cred", deserialize_json=True)['genius_cred']
            self.artist_name = artist
            self.song = song
            self.genius = lyricsgenius.Genius(self.geniusCreds)
            self.identifier_type = identifier_type
        
        def __iter__(self):
            return self
        
        def get_artist_all(self, max_songs=3):
            artist_data = self.genius.search_artist(self.artist_name, 
                                                    max_songs=max_songs, 
                                                    sort="title", 
                                                    include_features=True)
            return(artist_data)

        def get_lyrics(self):
            if self.identifier_type == "artist_song":
                song = self.genius.search_song(self.song, self.artist_name)
                return(song.lyrics)
            elif self.identifier_type == "artist":
                artist_songs = self.get_artist_all()#.songs
                songs = artist_songs.songs
                lyrics = {}
                for i in range(0, len(songs)):
                    lyrics[songs[i].title] = songs[i].lyrics
                    #lyrics.append(songs[i].lyrics)
                return(lyrics)

    log.info("Getting lyrics")

    s = """INSERT INTO spotify.lyrics(artist_name,track_name,track_id,lyrics) VALUES (%s, %s, %s, %s)"""

    for song in artist_tracks:
        obj = []
        
        try:
            lyrics_song = lyrics(artist=song[0], song=song[1], 
                             identifier_type="artist_song")
            lyrics_song = lyrics_song.get_lyrics()
        except:
            continue
        
        obj.append([song[0], song[1], song[2], lyrics_song])

        cursor.executemany(s, obj)
        conn.commit()                   
        #lyrics_artist_song = lyrics_artist_song + obj

    log.info("Ran query")

    conn.close()

# =============================================================================
# 3. Set up the dags
# =============================================================================
create_rest_tables = PythonOperator(
    task_id='create_rest_tables_in_db',
    python_callable=create_rest_tables_in_db,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)

insert_rows_artists = PythonOperator(
    task_id='insert_rows_artists',
    python_callable=insert_rows_artists,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)

insert_rows_albums = PythonOperator(
    task_id='insert_rows_albums',
    python_callable=insert_rows_albums,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)

insert_rows_tracks = PythonOperator(
    task_id='insert_rows_tracks',
    python_callable=insert_rows_tracks,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)

insert_rows_audio_features = PythonOperator(
    task_id='insert_rows_audio_features',
    python_callable=insert_rows_audio_features,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)

insert_rows_image_links = PythonOperator(
    task_id='insert_rows_image_links',
    python_callable=insert_rows_image_links,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)

insert_rows_lyrics = PythonOperator(
   task_id='insert_rows_lyrics',
   python_callable=insert_rows_lyrics,
   op_kwargs=default_args,
   provide_context=True,
   dag=dag,
)

# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================
create_rest_tables >> insert_rows_artists >> insert_rows_albums >> insert_rows_tracks >> insert_rows_audio_features >> insert_rows_image_links >> insert_rows_lyrics