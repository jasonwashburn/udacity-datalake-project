import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import to_timestamp


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    print("Processing Song Data...")
    # get filepath to song data file
    songs_path = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    songs_data = spark.read.json(songs_path)
    
    # extract columns to create songs table
    songs_df = songs_data.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    print('Writing songs to parquet.')
    songs_df.write.partitionBy('year', 'artist_id').mode('overwrite').parquet(output_data + 'songs')

    # extract columns to create artists table
    artists_df = songs_data.select(songs_data.artist_id, \
                                    songs_data.artist_name.alias('name'), \
                                    songs_data.artist_location.alias('location'), \
                                    songs_data.artist_latitude.alias('latitude'), \
                                    songs_data.artist_longitude.alias('longitude'))
    
    # drop duplicate artist records
    artists_df = artists_df.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    print("Writing artists to parquet.")
    artists_df.write.mode('overwrite').parquet(output_data + 'artists')



def process_log_data(spark, input_data, output_data):
    print("Processing Log Data...")
    # get filepath to log data file
    log_path = input_data + 'log-data/'

    # read log data file
    log_df = spark.read.json(log_path)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')
    
    # extract columns for users table    
    users_df = log_df.select(log_df.userId.cast("int").alias('user_id'), \
        log_df.firstName.alias('first_name'), \
        log_df.lastName.alias('last_name'), \
        log_df.gender, \
        log_df.level)

    # drop duplicate user records
    users_df = users_df.dropDuplicates(['user_id'])
    
    # write users table to parquet files
    print('Writing users to parquet.')
    users_df.write.mode('overwrite').parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    log_df = log_df.withColumn('timestamp', to_timestamp(log_df.ts / 1000))
    
    # extract columns to create time table
    df_time = log_df.select(log_df.timestamp.alias('start_time'), F.hour(log_df.timestamp).alias('hour'), F.dayofmonth(log_df.timestamp).alias('day'), \
                    F.weekofyear(log_df.timestamp).alias('week'), F.month(log_df.timestamp).alias('month'), \
                    F.year(log_df.timestamp).alias('year'), F.dayofweek(log_df.timestamp).alias('weekday'))

    # drop duplicates
    df_time = df_time.distinct()
    
    # write time table to parquet files partitioned by year and month
    print('Writing time to parquet.')
    df_time.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'time')


    # read in song data to use for songplays table
    songs_path = input_data + 'song_data/*/*/*/*.json'
    songs_data = spark.read.json(songs_path)

    # join song and log datasets to create songplays table 
    joined_df = log_df.join(songs_data, ((log_df.song == songs_data.title) & \
                        ((log_df.length == songs_data.duration) & \
                        (log_df.artist == songs_data.artist_name))), how = 'full')

    # join joined_df with time table to provide 
    joined_df = joined_df.withColumnRenamed('year', 'album_year').join(df_time, joined_df.timestamp == df_time.start_time)

    # extract columns for songplays_table
    song_plays = joined_df.select(joined_df.timestamp.alias('start_time'), \
                            joined_df.userId.cast('int').alias('user_id'), \
                            joined_df.level, \
                            joined_df.song_id, \
                            joined_df.artist_id, \
                            joined_df.sessionId.alias('session_id'), \
                            joined_df.artist_location.alias('location'), \
                            joined_df.userAgent.alias('user_agent'), \
                            joined_df.month, \
                            joined_df.year)

    # write songplays table to parquet files partitioned by year and month
    print('Writing songplays to parquet')
    song_plays.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = 'data/'
    output_data = "analytics/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
