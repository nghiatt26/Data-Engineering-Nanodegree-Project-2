import configparser

'''
    Get the data information for the dwh.cfg.
'''
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# VARIABLES
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE","ARN")

'''
    Drop staging, dimention and fact table before create new one.
'''

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_Events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_Songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_Songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_User"
song_table_drop = "DROP TABLE IF EXISTS dim_Song"
artist_table_drop = "DROP TABLE IF EXISTS dim_Artist"
time_table_drop = "DROP TABLE IF EXISTS dim_Time"

'''
    Create staging, dimention and fact table.
'''

# CREATE TABLES

# Create staging events table
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_Events
(
    artist          VARCHAR,
    auth            VARCHAR, 
    firstName       VARCHAR,
    gender          VARCHAR,   
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          FLOAT,
    level           VARCHAR, 
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    FLOAT,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    userAgent       VARCHAR,
    userId          INTEGER
);
""")

# Create staging songs table
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_Songs
(
    song_id            VARCHAR,
    num_songs          INTEGER,
    title              VARCHAR,
    artist_name        VARCHAR,
    artist_latitude    FLOAT,
    year               INTEGER,
    duration           FLOAT,
    artist_id          VARCHAR,
    artist_longitude   FLOAT,
    artist_location    VARCHAR
);
""")

# Create fact songs play table
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_Songplay
(
    songplay_id          INTEGER IDENTITY(0,1) PRIMARY KEY sortkey,
    start_time           TIMESTAMP,
    user_id              INTEGER,
    level                VARCHAR,
    song_id              VARCHAR,
    artist_id            VARCHAR,
    session_id           INTEGER,
    location             VARCHAR,
    user_agent           VARCHAR
);
""")

# Create dim user table
user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_User
(
    user_id         INTEGER PRIMARY KEY distkey,
    first_name      VARCHAR,
    last_name       VARCHAR,
    gender          VARCHAR,
    level           VARCHAR
);
""")

# Create dim song table
song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_Song
(
    song_id     VARCHAR PRIMARY KEY,
    title       VARCHAR,
    artist_id   VARCHAR distkey,
    year        INTEGER,
    duration    FLOAT
);
""")

# Create dim artist table
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_Artist
(
    artist_id          VARCHAR PRIMARY KEY distkey,
    name               VARCHAR,
    location           VARCHAR,
    latitude           FLOAT,
    longitude          FLOAT
);
""")

# Create dim time table
time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_Time
(
    start_time    TIMESTAMP PRIMARY KEY sortkey distkey,
    hour          INTEGER,
    day           INTEGER,
    week          INTEGER,
    month         INTEGER,
    year          INTEGER,
    weekday       INTEGER
);
""")

'''
    Insert data from S3 to staging table.
'''

# STAGING TABLES
# Insert data from S3 to staging events table
staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

# Insert data from S3 to staging songs table
staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE)

""""
    After insert data from S3 to staging table is successful. Then insert data form staging to the fact and dimention table. 
""""

# FINAL TABLES
# Insert data from staging events and staging songs to fact song play table
songplay_table_insert = ("""
INSERT INTO fact_Songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(E.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                E.userId as user_id,
                E.level as level,
                S.song_id as song_id,
                S.artist_id as artist_id,
                E.sessionId as session_id,
                E.location as location,
                E.userAgent as user_agent
FROM staging_Events E
JOIN staging_Songs S ON E.song = S.title AND E.artist = S.artist_name;
AND se.page = 'NextSong';
""")

# Insert data from staging events to dim user table
user_table_insert = ("""
INSERT INTO dim_User(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId as user_id,
                firstName as first_name,
                lastName as last_name,
                gender as gender,
                level as level
FROM staging_Events
where userId IS NOT NULL;
AND page = 'NextSong';
""")

# Insert data from staging songs to dim song table
song_table_insert = ("""
INSERT INTO dim_Song(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
FROM staging_Songs
WHERE song_id IS NOT NULL;
""")

# Insert data from staging songs to dim artist table
artist_table_insert = ("""
INSERT INTO dim_Artist(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM staging_Songs
where artist_id IS NOT NULL;
""")

# Insert data from staging events to dim time table
time_table_insert = ("""
INSERT INTO dim_Time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
FROM staging_Events
WHERE ts IS NOT NULL;
""")

'''
    Create query to run the project
'''

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
