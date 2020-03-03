import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender char,
        iteminSession integer,
        lastName varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration numeric,
        sessionId integer sortkey,
        song varchar,
        status integer,
        ts bigint,
        userAgent varchar,
        userId integer);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs integer,
        artist_id varchar,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar,
        artist_name varchar,
        song_id varchar sortkey,
        title varchar,
        duration numeric,
        year integer);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id integer identity(0,1) primary key,
        start_time timestamp not null sortkey,
        user_id integer not null,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id integer not null,
        location varchar,
        user_agent varchar);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id integer primary key sortkey,
        first_name varchar,
        last_name varchar,
        gender char,
        level varchar distkey);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar primary key sortkey,
        title varchar not null,
        artist_id varchar not null,
        year integer distkey,
        duration numeric);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar sortkey,
        name varchar not null,
        location varchar distkey,
        latitude numeric,
        longitude numeric);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp primary key sortkey distkey,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday integer);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    json '{}' region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    json 'auto' region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent)

        select
        timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time,
        e.userId as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId as session_id,
        e.location,
        e.userAgent as user_agent
        from staging_events e
        join staging_songs s on s.artist_name = e.artist
            and s.title = e.song
        where e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level)

        select distinct
        userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
        from staging_events
        where page = 'NextSong'
        and userId is not null;
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration)

        select distinct
        song_id,
        title,
        artist_id,
        year,
        duration
        from staging_songs
        where song_id is not null;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude)

        select distinct
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
        from staging_songs
        where artist_id is not null;
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday)

        select distinct
        timestamp 'epoch' + ts/1000 * interval '1 second' as start_time,
        date_part(h, timestamp 'epoch' + ts/1000 * interval '1 second') as hour,
        date_part(d, timestamp 'epoch' + ts/1000 * interval '1 second') as day,
        date_part(w, timestamp 'epoch' + ts/1000 * interval '1 second') as week,
        date_part(mon, timestamp 'epoch' + ts/1000 * interval '1 second') as month,
        date_part(y, timestamp 'epoch' + ts/1000 * interval '1 second') as year,
        date_part(dow, timestamp 'epoch' + ts/1000 * interval '1 second') as weekday
        from staging_events
        where page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
