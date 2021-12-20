# DROP TABLES

songplay_table_drop = "DROP table IF Exists songplays "
user_table_drop = "DROP table IF Exists users"
song_table_drop = "DROP table IF Exists songs"
artist_table_drop = "DROP table IF Exists artists"
time_table_drop = "DROP table IF Exists time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
songplay_id SERIAL PRIMARY KEY NOT NULL, 
start_time timestamp, 
user_id int NOT NULL, 
level varchar, 
song_id varchar, 
artist_id varchar, 
session_id int,
location varchar, 
user_agent varchar);""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY NOT NULL, 
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar);""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS  songs (
song_id varchar PRIMARY KEY, 
title varchar, 
artist_id varchar, 
year int, 
duration float);""")


artist_table_create = ("""
CREATE TABLE IF NOT EXISTS  artists (
artist_id varchar PRIMARY KEY NOT NULL, 
name varchar, 
location varchar, 
latitude numeric, 
longitude numeric);""")


time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
start_time timestamp PRIMARY KEY NOT NULL,
hour int, 
day int, 
week int, 
month int,
year int,
weekday int);""")

# INSERT RECORDS

songplay_table_insert = ("""
Insert into songplays 
(
    start_time, user_id, level, song_id, artist_id, session_id,location, user_agent
) 
VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
""")

user_table_insert = ("""
Insert into users 
(
    user_id,first_name, last_name, gender, level
) 
VALUES (%s,%s,%s,%s,%s) 
ON CONFLICT (user_id) DO UPDATE SET level=Excluded.level;
""")

song_table_insert = ("""
Insert into songs
(
    song_id, title,artist_id,year,duration
) 
VALUES (%s, %s, %s, %s,%s) 
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
Insert into artists
(
    artist_id, name, location, latitude, longitude
) 
VALUES (%s, %s,%s,%s,%s) 
ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
Insert into time
(
    start_time,hour,day, week, month, year, weekday
) 
VALUES (%s,%s,%s,%s,%s,%s,%s) 
ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
Select so.song_id, so.artist_id from songs so 
INNER JOIN artists ar 
ON so.artist_id=ar.artist_id 
where so.title= %s and ar.name = %s and so.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
