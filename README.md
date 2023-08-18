# 📐✏️ Architecture
![alt text](https://github.com/prince-daniel/spotify-etl/blob/main/Architecture.jpg)
## 🏗️ Data Extraction 
The project is hosted on an EC2 instance. Asynchronous API calls are utilized to interact with the Spotify API, efficiently extracting data from a diverse range of around 155 artists.

## 🗄️ ETL Operations Data Storage
Leveraging the flexibility of DynamoDB, I've set up a table with flag columns that seamlessly support the ETL process.

## 📦 Raw Data Storage 
Extracted data is then stored in an S3 raw bucket. I've chosen the optimized Parquet file format to ensure efficient storage.

## 🔄 Transformation with Databricks 
The power of Databricks is harnessed for data transformation. The raw data residing in the S3 bucket is ingested, transformed, and optimized, resulting in a refined dataset ready for serving.

## 📅 Delta Tables
The transformed data is stored as an external delta table, Harnessing the features & capabilities of Delta Lake.

## 🚚 Data Loading into Redshift 
Python script is utilized to execute the COPY command, efficiently transferring the data into Amazon Redshift. This robust process ensures that the data is available for further exploration and visualization.
  
# ⚒️ Redshift DDL
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
DISTKEY (artist_id)
SORTKEY (album_id);
```

# 📊 Dashboard
![alt text](https://github.com/prince-daniel/spotify-etl/blob/main/TheWeeknd.jpg)

#### 🙆‍♂️I'll add more artists in the future
