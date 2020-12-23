"""Populates sparkifydb database with song and log data.

Run this script to populate the sparkifydb database with information parsed
from data/song_data and data/log_data. create_tables.py must be run first to
ensure the tables are empty.
"""

import os
import glob
from typing import Callable
import psycopg2
import pandas as pd
from sql_queries import *

# psycopg2 types
PGCursor: psycopg2.extensions.cursor = psycopg2.extensions.cursor
PGConnection: psycopg2.extensions.connection = psycopg2.extensions.connection


def process_song_file(cur: PGCursor, filepath: str) -> None:
    """Populates postgres database with song details parsed from a JSON file.

    Args:
        cur: A psycopg2 database cursor.
        filepath: A JSON filepath.
    """

    # open song file
    df = pd.DataFrame([pd.read_json(filepath, typ="series")])

    # insert artist record
    artist_data = df.loc[:, ["artist_id", "artist_name", "artist_location",
                             "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df.loc[:, ["song_id", "title", "artist_id",
                           "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)


def process_log_file(cur: PGCursor, filepath: str) -> None:
    """Populates postgres database with log details parsed from a JSON file.

    Args:
        cur: A psycopg2 database cursor.
        filepath: A JSON filepath.
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    t = df["ts"]

    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.isocalendar(
    ).week, t.dt.month, t.dt.year, t.dt.day_name()]
    column_labels = ["timestamp", "hour", "day",
                     "week of year", "month", "year", "weekday"]
    time_series = [series.rename(column_labels[i])
                   for i, series in enumerate(time_data)]

    time_df = pd.concat(time_series, axis=1)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ["userId", "firstName",
                         "lastName", "gender", "level"]].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid,
                         artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur: PGCursor,
                 conn: PGConnection,
                 filepath: str,
                 func: Callable[[PGCursor, str], None]) -> None:
    """Reads song and log data into sparkifydb.

    Args:
        cur: A psycopg2 database cursor.
        conn: A psycopg2 database connection.
        filepath: A filepath to a directory containing JSON files.
        func: A processing fn (process_song_file or process_log_file).
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main() -> None:
    """Connects to sparkifydb and populates tables with song and log data"""

    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
