-- =====================================================================
-- clean_streaming.pig
-- Apache Pig ETL script — Stray Kids Spotify Streaming Analytics
-- Run as: pig -x mapreduce clean_streaming.pig
-- Reads:  /user/maria_dev/spotify/src_streaming/spotify_skz_streaming_4.25_4.26.csv
-- Writes: /user/maria_dev/spotify/processed_streaming/cleaned_output
-- =====================================================================

-- Load the raw data from the verified uniform lowercase path
raw_data = LOAD '/user/maria_dev/spotify/src_streaming/spotify_skz_streaming_4.25_4.26.csv' USING PigStorage(',') AS (
    end_time:chararray, end_year:int, end_month:int, end_date:int, end_day:chararray, 
    end_hour_utc:int, end_hour_pst:int, artist_name:chararray, track_name:chararray, 
    album_name:chararray, release_year:int, release_country:chararray, album_type:chararray, 
    played_ms:long, played_min:chararray, played_h:chararray
);
-- Filter out textual headers and listening records shorter than 30 seconds (30000 ms)
cleaned_logs = FILTER raw_data BY end_time != 'end_time' AND played_ms > 30000;
-- Transform values and inject safe, native pseudo BPM features based on track title string size
final_etl_stream = FOREACH cleaned_logs GENERATE 
    end_time, end_year, end_month, end_date, end_day, end_hour_utc, end_hour_pst,
    artist_name, track_name, album_name, release_year, release_country, album_type,
    played_ms, played_min, played_h,
    (int)((SIZE(track_name) * 3) % 60 + 80) AS bpm;
-- Save the refined data rows directly into the processed folder location
STORE final_etl_stream INTO '/user/maria_dev/spotify/processed_streaming/cleaned_output' USING PigStorage(',');
