# Project: Data Warehouse

## Description
The purpose of this project is to enable analytics to be more easily performed 
on log data created by a music streaming app called sparkify.

Utilizing Spark on EMR, log files are read from S3 and parsed into user, time, and 
songplay tables. Additionally, song data from the million songs database contained
on S3 is parsed into song and artist tables. All tables are written back into an S3 
bucket in parquet for later analytical use.

## Usage

### etl.py
This script is run on an EMR cluster. It will read both log and song data from an
S3 bucket, convert the data into song, artist, users, time, and songplays tables
then output them to an S3 bucket as parquet files.

Note: can be run locally on smaller datasets for development by changing the 
input and outputh paths and/or uncommenting a block of code at the beginning 
that parses dl.cfg for AWS ID and secret key to enable access to S3.

### dl.cfg
Contains configuration data needed to connect to S3 if script is running on a 
local machine (ie: for development)

## Database Design
The database design schema consists of the following tables:

    songplays - contains a consolidated list of song play activity for analysis
        |-- start_time: timestamp 
        |-- user_id: integer 
        |-- level: string 
        |-- song_id: string 
        |-- artist_id: string 
        |-- session_id: long 
        |-- location: string 
        |-- user_agent: string 
        |-- month: integer 
        |-- year: integer 

    users - contains data on sparkify users derived from log files in ./data/log_data
        |-- user_id: integer 
        |-- first_name: string 
        |-- last_name: string 
        |-- gender: string 
        |-- level: string 

    songs - contains details on songs from song files in ./data/song_data
        |-- song_id: string 
        |-- title: string 
        |-- artist_id: string 
        |-- year: long 
        |-- duration: double 

    artists - contains details on artists from song files in ./data/song_data
        |-- artist_id: string 
        |-- name: string 
        |-- location: string 
        |-- latitude: double 
        |-- longitude: double 

    time - contains a non-duplicate list of timestamps and converted time data
        |-- start_time: timestamp 
        |-- hour: integer 
        |-- day: integer 
        |-- week: integer 
        |-- month: integer 
        |-- year: integer 
        |-- weekday: integer 