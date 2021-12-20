import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    
    """
    Description:
    
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.
    
    Arguments:
    
    cur - cursor variable
    filepath - song file filepath
    
    """
    # open song file
    #song_files=filepath[0]
    #print(filepath)
    df = pd.read_json(filepath,typ='series')
    #df2=df.loc[:,["song_id","title","artist_id","year","duration"]]

    # insert song record
    #song_data = df2.values[0].tolist() -- gets only values from single file
    song_data = df[["song_id","title","artist_id","year","duration"]]
    #print(song_data)
    cur.execute(song_table_insert, song_data)
    
    
    # insert artist record
    #df3=df.loc[:,["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]]
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    #artist_data = df3.values[0].tolist() -- gets only values from single file
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
    Description:
    
    This procedure process the logfile whose filepath has been provided as an arugment.
    It extracts the data that is required to load the time, user and songplay tables
    
    Arguments:
    
    cur - cursor variable
    filepath - song file filepath
    
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    filter=['NextSong']
    df_t = df.loc[df['page'].isin(filter)] 

    # convert timestamp column to datetime
    t = pd.to_datetime(df_t['ts'])
    #ts1 = t.values.tolist()
    t_month=t.dt.month
    #print(t_month)
    t_year=t.dt.year
    #print(t_year)
    t_hour=t.dt.hour
    #print(t_hour)
    t_day=t.dt.day
    #print(t_day)
    t_weekofyear=t.dt.weekofyear
    #print(t_weekofyear)
    t_weekday=t.dt.weekday
    #print(t_weekday)
    
    # insert time data records
    time_data={"timestamp":t,
          "hour" : t_hour,
          "day" : t_day,
          "weekofyear" : t_weekofyear,
          "month" : t_month,
          "Year" : t_year,
          "Weekday" : t_weekday}
    
    time_df = pd.DataFrame.from_dict(time_data) 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    u_df = df.loc[:,["userId","firstName","lastName","gender","level"]]
    user_df = u_df.replace(to_replace = "",value="0")

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    df1_new=df.replace(to_replace = "",value="0") 
    for index, row in df1_new.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data =(pd.to_datetime(row.ts,unit='ms'),row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    
    """
    Description:
    
    This procedure gets all the files from the directory and the data will be ingected int o the respectiver tables based on the passed func argument
    
    Argument:
    
    cur - cursor variable
    conn - conection varaible
    filepath - directory path
    func - function that needs to be called i.e process_song_file/process_log_file
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()