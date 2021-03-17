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
	"""
	Create rest tables in postgress database
	"""
	#Get the postgress credentials from the airflow connection, establish connection 
	#and initiate cursor
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    log.info('Initialised connection')

    #Text for query to create tables
    #Drop each table before creating, as we want data from previous runs in this 
    #case to be erased
    #associate the tables with references to other tables
    #set primary keys
    query = """ 
        CREATE SCHEMA IF NOT EXISTS spotify;

		DROP TABLE IF EXISTS spotify.artists;
		CREATE TABLE IF NOT EXISTS spotify.artists (
		    artist_id     varchar(256) primary key,
		    artist_name   varchar(256),
		    followers     varchar(256),
		    popularity    varchar(256)
		);

		DROP TABLE IF EXISTS spotify.albums;
		CREATE TABLE IF NOT EXISTS spotify.albums (
		    album_id       varchar(256) primary key,
		    artist_id      varchar(256) references spotify.artists(artist_id),
		    album_group    varchar(256),
		    album_type     varchar(256),
		    album_name     varchar(256)
		);

		DROP TABLE IF EXISTS spotify.albums_images_links;
		CREATE TABLE IF NOT EXISTS spotify.albums_images_links (
		    album_id     varchar(256) primary key references spotify.albums(album_id),
		    image_number int,
		    height       int,
		    width        int,
		    url          varchar(256)
		);

		DROP TABLE IF EXISTS spotify.tracks;
		CREATE TABLE IF NOT EXISTS spotify.tracks (
		    track_id     varchar(256) primary key ,
		  album_id     varchar(256) references spotify.albums(album_id),
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
		    track_id        varchar(256) primary key references spotify.tracks(track_id),
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
		  track_id    varchar(256) references spotify.tracks(track_id),
		  lyrics      varchar(10485760)
		);
    """

    #Execute the query and commit it to the database
    cursor.execute(query)
    conn.commit()
    log.info("Created Schema and Tables")

def insert_rows_artists(**kwargs):
	#Connect to amazon s3 bucket with the credentials in airflow connections
    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info("Established connection to S3 bucket")

    #Get the bucket name that a csv containing the 
    #names of the artists resides in
    bucket_name = kwargs['bucket_name']

    #Get from the variables set in airflow the path to the bucket
    #and get the 'key' argument of the dictionary
    key = Variable.get("spotify_get_csv_artists", deserialize_json=True)
    key = key['key']

    #Set the whole path to the folder of the csv
    paths = s3.list_keys(bucket_name=bucket_name, prefix="artist_names/")
    log.info("{}".format(paths))

    log.info("Reading CSV File artist names")
    #Get the file and split it into a list by '\r\n' and select the 2nd to last argument
    artist_names = s3.read_key(key, bucket_name)
    artist_names = artist_names.split('\r\n')[1:-1]

    log.info("Read artists names")
    log.info("{}".format(artist_names))

    #Get the credentials for the api to be used for scraping
    client_id =  Variable.get("spotify_client_id", deserialize_json=True)['client_id_sp']
    log.info("Got client_id")
    client_secret =  Variable.get("spotify_client_secret", deserialize_json=True)['key']
    log.info("Got client secret")

    #Initiate the credentials with the function provided by spotify and create the connection
    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.client.Spotify(client_credentials_manager=client_credentials_manager)

    log.info("Established connection to sp")

    def get_artists(query):
        '''
        function that extracts all artists from spotify with an exact name"query"
        input: query (artist name)
        output: list containing 4 elements about the artist
        '''

        #initiate a list to put the data extracted
        artists_list=[]
    
    	#Search for the artist and save into variable "ar" the data related to artists
        results=sp.search(q=query,type='artist') # json format 
        ar=results['artists'] 
    	
    	#for the data only in items
        items=ar['items']
        for it in items:
        	#if we have an exact match
            if it['name'] == query:
            	#create a variable for every kind of data needed
                artist_id = it['id']
                artist_name = it['name']
                followers = it['followers']['total']
                popularity = it['popularity']
                #put them all inm a list
                artist = [artist_id,artist_name,followers,popularity]
                #append to the list containing the data for all exact matches
                artists_list.append(artist)
        
        #return the list
        return artists_list

    log.info("Running queries in sp API")
    
	#a list for all the artists extracted from spotify
    artists = []
    
    #for each artist name
    for query in artist_names:
    	#get the data of the artist
        result_artists = get_artists(query)

        #add them so we have a list of lists
        artists = artists + result_artists
    
    log.info('Ran queries in sp API')
    log.info('{}'.format(artists))

    #initiate a connection to postgress, create a connection and initiate a cursor
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    #this query will insert into the table the arguments in every list by order
    s = """INSERT INTO spotify.artists (artist_id, artist_name, followers, popularity) VALUES (%s, %s, %s, %s)"""
    log.info('Updating rows in artists table')

    #execute for all (many) the lists within the list 'artists'
    cursor.executemany(s, artists)
    conn.commit()
    log.info('Updated rows in artists table')

	#close the connection to postgres
    conn.close()

def insert_rows_albums(**kwargs):
	"""
	Insert data into the albums table

	Gets data from the artists table created before, 
	queries the spotify API and gets album data for each artist
	Inserts them all in the database
	"""

	#Get the spotify credentials for airflow
    client_id =  Variable.get("spotify_client_id", deserialize_json=True)['client_id_sp']
    log.info("Got client_id")
    client_secret =  Variable.get("spotify_client_secret", deserialize_json=True)['key']
    log.info("Got client secret")

    #Initiate the credentials with the function provided by spotify and create the connection
    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    log.info("Established connection to sp")

    #initiate a connection to postgress, create a connection and initiate a cursor
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # select artist_id from artists table

    #Get artist ids from the artists table
    q = "SELECT artist_id FROM spotify.artists;"
    cursor.execute(q)
    conn.commit()
    artist_ids = cursor.fetchall()

    # get albums
    def get_albums(artist):
        """
		Extracts from the API all the albums relating to an artist ID

		input: str of artist id
		output: list with album details
        """
        #create an empty list for albums to be put in
        artist_albums=[]
        #search and get albums with the artist id
        results=sp.artist_albums(artist)
        
        #contains a list of all the artist albums
        items=results['items']

        #Iterate across the albums in items        
        for it in items:
        	#save each value
            album_id=it['id']
            album_group=it['album_group']
            album_type=it['album_type']
            album_name=it['name']
            release_date=it['release_date']
            total_tracks=it['total_tracks']
            artist_id=artist

    		#Place them in a list
            album=[album_id,artist_id,album_group,album_type,album_name,release_date,total_tracks]
            #Append to the rest of the albums of the artist
            artist_albums.append(album)
        #return the list with all the albums with the artist id
        return artist_albums

    log.info("Running queries in sp API")
    #create an empty list to put all the albums in
    albums_list=[]
    #turn the album ids we got from the database to a list of that can be 
    #iterated with mapping every element
    map_artist_ids = map(list, artist_ids)

    #for every artist id
    for artist in map_artist_ids:
    	#get all the albums
    	#for each artist we have the id and a coma (that is returned by sql)
    	#select just the artist id (1st element)
        artist_albums=get_albums(artist[0])

        #add them to a list (list of lists)
        albums_list=albums_list+artist_albums
    log.info('Ran queries in sp API')
 
 	#turn the results in a dataframe
    albums_list = pd.DataFrame(albums_list)

    #if an album does not have a day of month, place that it was issued at the first of the month
    #if it doesn't have month and date, place it as the first day of the first month of the year
    #to avoid formating errors with sql
    albums_list.iloc[:, -2] = albums_list.iloc[:, -2].apply(lambda x: x + '-01-01' if len(x) == 4 else(x + '-01' if len(x) == 7 else x))

    #this query will insert into the table the arguments in every list by order
    s = """INSERT INTO spotify.albums(album_id,artist_id,album_group,album_type,album_name,release_date,total_tracks) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    log.info('Updating rows in albums table')

    #execute for all (many) values of each column of the dataframe 'albums list'
    cursor.executemany(s, list(albums_list.values))
    conn.commit()
    log.info('Updated rows in albums table')

    #close the connection to postgres
    conn.close()

def insert_rows_tracks(**kwargs):
	"""
	Insert rows into tracks table with data about each song

	Query the albums table and gets the album id of each album
	Query the spotify API for each album
	Insert the data of the songs of each album in the database 
	Avoids inserting all the data of the songs as above together to avoid RAM limitations
	"""

	#Get the spotify credentials for airflow
    client_id =  Variable.get("spotify_client_id", deserialize_json=True)['client_id_sp']
    log.info("Got client_id")
    client_secret =  Variable.get("spotify_client_secret", deserialize_json=True)['key']
    log.info("Got client secret")

    #Initiate the credentials with the function provided by spotify and create the connection
    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    log.info("Established connection to sp")

    #initiate a connection to postgress, create a connection and initiate a cursor
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    log.info("Got connection to postgres")

    # select album from albums table
    q = "SELECT album_id FROM spotify.albums;"
    cursor.execute(q)
    conn.commit()
    album_ids = cursor.fetchall()
    log.info("Got album_ids")

    # get tracks
    def get_tracks(album):
        """
        a function that extracts from spotify all tracks of a given album

		input: album id
		output: list of containing lists with data of each song
        """
        #initiate a list to put the data for each album
        album_tracks=[]

        #search the spotify API for tracks of each album
        results=sp.album_tracks(album)

        #list of all the album tracks
        items=results['items']#contains a list of all the album tracks
        
        #iterate acrross album tracks
        for it in items:
        	#save each attribute in a variable
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

            #consolidate the variables in a list
            track=[track_id,album_id,disc_number,duration_ms,explicit,is_local,track_name,preview_url,track_number,track_type]

            #append the each track to a lsit containing all the album tracks
            album_tracks.append(track)

        #return in list of lists the album tracks
        return album_tracks


    #turn the album ids we got from the database to a list of that can be 
    #iterated with mapping every element
    album_ids = map(list, album_ids)

    #this query will insert into the table the arguments in every list by order
    s = """INSERT INTO spotify.tracks(track_id,album_id,disc_number,duration_ms,explicit,is_local,track_name,preview_url,track_number,track_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    log.info("Getting album tracks")
    log.info("Getting album tracks and inserting to table")
    #for each album id
    for album in album_ids:
    	#create an empty list
        tracks_list=[]

        #get all the tracks for each album
        #for each album we have the id and a coma (that is returned by sql)
    	#select just the artist id (1st element), not the comma
        album_tracks=get_tracks(album[0])

        #add the tracks in the list above
        tracks_list=tracks_list+album_tracks

        #Insert them now to the table
        #we don't want many tracks at once in RAM
        cursor.executemany(s, list(tracks_list))
        conn.commit()

    #close the connection
    conn.close()

def insert_rows_audio_features(**kwargs):
	"""	
	Insert into the audio features table data about the musical characteristics about each song

	Queries the track table for track ids
	Queries the spotify API for the characteristics of each song
	Insert for each song the data into the database to avoid ram limitations

	"""
	#Get the spotify credentials for airflow	
    client_id =  Variable.get("spotify_client_id", deserialize_json=True)['client_id_sp']
    log.info("Got client_id")
    client_secret =  Variable.get("spotify_client_secret", deserialize_json=True)['key']
    log.info("Got client secret")

    #Initiate the credentials with the function provided by spotify and create the connection
    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    log.info("Established connection to sp")

    #initiate a connection to postgress, create a connection and initiate a cursor
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    #get all the track ids from the tracks table
    q = "SELECT track_id FROM spotify.tracks;"
    cursor.execute(q)
    conn.commit()
    track_ids = cursor.fetchall()
    log.info("Got track ids")

    # get audio features
    def get_audio_features(track):
    	"""
		Gets audio features for given song

		Input: track id
		Output: list of audio features
    	"""
        #query the spotify API for the audio features of each song
        results=sp.audio_features([track])
        #remove spaces
        re=results.pop()

        #save each feature in a variable
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
        
        #place them in a list
        features=[audio_features_id,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,
        		  liveness,valence,tempo,register_type,duration_ms,time_signature]
        
        #return the list
        return features


    #turn the track ids we got from the database to a list of that can be 
    #iterated by mapping every element
    map_track_ids = map(list, track_ids)

    #this query will insert into the table the arguments in every list by order    
    s = """INSERT INTO spotify.audio_features(track_id,danceability,energy,key,loudness,mode,speechiness,acousticness,intrumentalness,liveness,valence,tempo,register_type,duration_ms,time_signature) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    log.info("Getting track features")
    #for every track ID
    for track in map_track_ids:
        #if it finds the audio features of a song return them in a variable
        try:
        	#track contains the track id and a comma that
        	#we get when querying postgress. here select just the id
        	#and use it in the function above
            audio_features=get_audio_features(track[0])
        except:
        	#else continue to the next song
            continue

		#Insert them now to the table
        #we don't want many tracks at once in RAM
        cursor.executemany(s, list(audio_features))
        conn.commit()        

    log.info("Ran query")

    #close the connection
    conn.close()

def insert_rows_image_links(**kwargs):
	"""	
	Insert into the images links table data about the image of an album

	Queries the albums table for album ids
	Queries the spotify API for the photo of each album
	Insert for each album the data into the database
	"""	

	#Get the spotify credentials for airflow	
    client_id =  Variable.get("spotify_client_id", deserialize_json=True)['client_id_sp']
    log.info("Got client_id")
    client_secret =  Variable.get("spotify_client_secret", deserialize_json=True)['key']
    log.info("Got client secret")

    #Initiate the credentials with the function provided by spotify and create the connection
    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    log.info("Established connection to sp")

    #initiate a connection to postgress, create a connection and initiate a cursor
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    #Get album id for each album
    q = "SELECT album_id FROM spotify.albums;"
    cursor.execute(q)
    conn.commit()
    album_ids = cursor.fetchall()
    log.info("Got album_ids")

    # get image links
    def get_images_links(album):
        """
		Get data for images related to each album

		Input: album id
		Output: list of lists with the data about the images of the album
        """
        #initiate a list for the images
        images_links=[]

        #Seach the spotify API
        results=sp.album(album)
        #Get the data about images
        images=results['images']
        
        #count each image of each album
        image_number=1

        #for every image
        for im in images:
        	#save attributes in variables
            height=im['height']
            url=im['url']
            width=im['width']
            album_id=album

            #create a list of all the variables
            image=[album_id,image_number,height,width,url]

            #append them to all the images of the album
            images_links.append(image)
            #add one to the number of the image
            image_number=image_number+1

        #return list of lists of the images of the album
        return images_links

    #turn the album ids we got from the database to a list of that can be 
    #iterated with mapping every element
    album_ids = map(list, album_ids)

    #this query will insert into the table the arguments in every list by order    
    s = """INSERT INTO spotify.albums_images_links(album_id,image_number,height,width,url) VALUES (%s, %s, %s, %s, %s)"""

    log.info("Getting image linkes")
    for album_id in album_ids:
        #for each album we have the id and a coma (that is returned by sql)
    	#select just the artist id (1st element), not the comma and use the function above
        images_links=get_images_links(album_id[0])

        #insert the data in the database
        cursor.executemany(s, images_links)
        conn.commit()

    log.info("Run query")

    #close the connection
    conn.close()

def insert_rows_lyrics(**kwargs):
	"""	
	Insert into the lyrics table data about the lyrics of a song

	Queries the tracks table for track ids
	Queries the genius API for the lyrics of each song
	Insert for each song the data into the database
	"""	

    #initiate a connection to postgress, create a connection and initiate a cursor	
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    """
	In order to get the lyrics we need the artist name and the name of the song

	With this query we get the artist ids and the albums of each artist
	We also get the tracks of each album along with their album ids

	Then join the results and end up with the artist name, track name and track ID
	(We don't need the track ID to do the query, but we need it to connect the data
	to the other tables since this is not part of the spotify environment. Since we trust
	Spotify track ids after checks we consider them as the id for the lyrics table)
    """
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

    #Execute the query
    cursor.execute(q)
    conn.commit()
    artist_tracks = cursor.fetchall()
    log.info("Got artist name, track name, track id")

    class lyrics:
    	"""
		Class that gets an artist name and a song name and create an object for it

		If identifier type is not set to artist_song, we get all the lyrics for the given artist

		Inputs: 
		artist = string with artist name
		song = string with song name 
		identifier type = if set to artist returns all the lyrics of a given artist, if set to
						  artist_song, returns the lyrics of a particular song

		Output:
		String containing Lyrics (for one of many song depending on identifier)
    	"""
        def __init__(self, artist, song=None, identifier_type = "artist"):
        	#initiate credentials we get from airflow
            self.geniusCreds = Variable.get("spotify_genius_cred", deserialize_json=True)['genius_cred']

            #initiate artist name and song name
            self.artist_name = artist
            self.song = song

            #set connection to the genius API
            self.genius = lyricsgenius.Genius(self.geniusCreds)

            #initiate identifier type
            self.identifier_type = identifier_type
        
        def __iter__(self):
            return self
        

        def get_artist_all(self, max_songs=3):
        	"""
			Returns all the lyrics of a given artist (first 3), 
			sorted by the song title
			
			Inputs: max_songs=set for number of songs to get
			Output: string with the lyrics of all the songs of an artist and 
					the artist name. before each piece of lyrics is the title
					and then the lyrics

        	"""
        	#query the genius API for the lyrics of songs of an artist
            artist_data = self.genius.search_artist(self.artist_name, 
                                                    max_songs=max_songs, 
                                                    sort="title", 
                                                    include_features=True)

            return(artist_data)

        def get_lyrics(self):
            """
			Get the all the lyrics of a given artist or a certain song
            """
            #if identifier song, return only the lyrics for the song
            if self.identifier_type == "artist_song":
            	#search the genius API for the lyrics of the song
                song = self.genius.search_song(self.song, self.artist_name)
                return(song.lyrics)
            elif self.identifier_type == "artist":
            	#set an object
                artist_songs = self.get_artist_all()
                #get the songs
                songs = artist_songs.songs
                #create a dictionary to insert the title and lyrics
                lyrics = {}
                #for every song
                for i in range(0, len(songs)):
                	#set the title as key and the lyrics as value
                    lyrics[songs[i].title] = songs[i].lyrics
            
                return(lyrics)

    log.info("Getting lyrics")

    #this query will insert into the table the arguments in every list by order        
    s = """INSERT INTO spotify.lyrics(artist_name,track_name,track_id,lyrics) VALUES (%s, %s, %s, %s)"""

    #for each song
    for song in artist_tracks:
        obj = []
        
        #if you can find the lyrics
        try:
        	#initiate an object
            lyrics_song = lyrics(artist=song[0], song=song[1], 
                             identifier_type="artist_song")
            #get the lyrics
            lyrics_song = lyrics_song.get_lyrics()
        except:
        	#or else continue to next song
            continue
        
        #append to the list initiated above the lyrics along
        #with artist name, song title and track id
        obj.append([song[0], song[1], song[2], lyrics_song])

        #insert the data in the table
        cursor.executemany(s, obj)
        conn.commit()                   

    log.info("Ran query")

    #Close the connection
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