# Architecture
![alt text](https://github.com/prince-daniel/spotify-etl/blob/main/Architecture.jpg)
- Asynchronous API calls are made to Spotify to extract the data.
- DynamoDB table has flag columns which supports the ETL process
- Data once extracted are stored in S3 raw bucket as parquet file.
- Raw bucket is then consumed by Databricks for Transformation.
- Transformed data are stored as external delta table (Some ad-hoc queries can be run on the delta table)
- Data is then loaded onto Redshift using Python by executing COPY command.
  
# Redshift DDL
```
CREATE TABLE spotify.albums (
    id TEXT,
    name TEXT,
    artist_name TEXT,
    total_tracks INTEGER,
    label TEXT,
    popularity INTEGER,
    href TEXT,
    image TEXT
)
DISTKEY(id);

CREATE TABLE spotify.artists (
    id TEXT,
    name TEXT,
    popularity INTEGER,
    genres TEXT,
    followers INTEGER,
    href TEXT,
    image TEXT
)
DISTKEY (id);

CREATE TABLE spotify.tracks (
    id TEXT,
    name TEXT,
    popularity INTEGER,
    duration_ms INTEGER,
    minute INTEGER,
    second INTEGER,
    explicit BOOLEAN,
    artist_id TEXT,
    album_id TEXT,
    release_date DATE,
    release_date_precision TEXT,
    image TEXT,
    href TEXT
)
DISTKEY (artist_id) -- Distribute by artist ID
SORTKEY (album_id);
```

# Dashboard
![alt text](https://github.com/prince-daniel/spotify-etl/blob/main/TheWeeknd.jpg)
