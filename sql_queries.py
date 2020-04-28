import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= """
CREATE TABLE staging_events(
artist VARCHAR(255) encode text255,
auth VARCHAR(255) encode text255,
firstName VARCHAR(100),
gender VARCHAR(10),
itemInSession INTEGER,
lastName VARCHAR(100),
length numeric,
level VARCHAR(50),
location VARCHAR(255) encode text255,
method VARCHAR(10),
page VARCHAR(50),
registration varchar(100),
sessionId INTEGER,
song VARCHAR(255),
status INTEGER,
ts BIGINT,
userAgent VARCHAR(255) encode text255,
userId INTEGER
) diststyle even;"""

staging_songs_table_create = """
CREATE TABLE staging_songs(
num_songs int,
artist_id text,
artist_lattitude numeric,
artist_longitude numeric,
artist_location text,
artist_name varchar(max),
song_id text,
title text,
duration numeric,
year int
)"""

songplay_table_create = """
CREATE TABLE songplays(
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time BIGINT REFERENCES time(start_time),
user_id INTEGER REFERENCES users(user_id),
level VARCHAR(50),
song_id text REFERENCES songs(song_id),
artist_id text REFERENCES artists(artist_id),
session_id INTEGER,
location VARCHAR(255),
user_agent VARCHAR(255)
)"""

user_table_create = """
CREATE TABLE users(
user_id INTEGER PRIMARY KEY,
first_name VARCHAR(100),
last_name VARCHAR(100),
gender VARCHAR(10),
level VARCHAR(50)
)"""

song_table_create = """
CREATE TABLE songs(
song_id text PRIMARY KEY,
title text,
artist_id text,
year INTEGER,
duration numeric
)"""

artist_table_create = """
CREATE TABLE artists(
artist_id text PRIMARY KEY,
name varchar(max),
location text,
lattitude numeric,
longitude numeric
)"""

time_table_create = """
CREATE TABLE time(
start_time timestamp PRIMARY KEY,
hour varchar(25),
day smallint,
week smallint,
month integer,
year smallint,
weekday boolean
)"""

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/log_data'
credentials 'aws_iam_role={}'
region 'us-west-2' compupdate off 
format as JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get('IAM_ROLE', 'ARN'))


staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={}'
region 'us-west-2'
format as JSON 'auto' truncatecolumns
""").format(config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT events.ts, events.userId, events.level,  songs.song_id, songs.artist_id, events.sessionId, events.location, events.userAgent
FROM staging_events as events
JOIN staging_songs as songs ON (events.artist=songs.artist_name)
                            AND (events.song=songs.title)
                            AND (events.length = songs.duration)
                            WHERE events.page='NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT userId, firstName, lastName, gender, level
FROM staging_events
WHERE staging_events.userId is not NULL;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, lattitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_lattitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT 
atime.start_time, 
EXTRACT(HOUR from atime.start_time), 
EXTRACT(DAY from atime.start_time),
EXTRACT(WEEK from atime.start_time),
EXTRACT(MONTH from atime.start_time),
EXTRACT(YEAR from atime.start_time),
EXTRACT(DOW from atime.start_time)
FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time FROM staging_events) as atime;
""")

# QUERY LISTS

#create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
