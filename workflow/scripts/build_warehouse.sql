-- =====================================================================
-- build_warehouse.sql
-- Apache Hive warehouse build — Stray Kids Spotify Streaming Analytics
-- Run as: beeline -f build_warehouse.sql  (or via Hive JDBC client)
-- Reads:  /user/maria_dev/spotify/processed_streaming  (Pig ETL output)
-- Builds: fact_streams_detail, dim_time_counts, dim_albums
-- =====================================================================

SET tez.runtime.io.sort.mb=60;
SET hive.tez.container.size=1024;
SET hive.tez.java.opts=-Xmx800m;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Force drop the existing database metadata catalog to reset the data warehouse layer
DROP DATABASE IF EXISTS skz_spotify_db CASCADE;

-- Create a fresh database entity and configure the runtime to point to it
CREATE DATABASE skz_spotify_db;
USE skz_spotify_db;

-- Enable core configurations for non-strict dynamic partitioning across clusters
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Define an external staging table pointing exactly to Pig output data stream
CREATE EXTERNAL TABLE tmp_staging (
    end_time STRING, end_year INT, end_month INT, end_date INT, end_day STRING, end_hour_utc INT, end_hour_pst INT, 
    artist_name STRING, track_name STRING, album_name STRING, release_year INT, release_country STRING, 
    album_type STRING, played_ms BIGINT, played_min STRING, played_h STRING, bpm INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/spotify/processed_streaming'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Define the partitioned core fact table schema mapped inside warehouse directory
CREATE EXTERNAL TABLE fact_streams_detail (
    end_time STRING, end_day STRING, end_hour_utc INT, end_hour_pst INT, artist_name STRING, 
    track_name STRING, album_name STRING, release_year INT, release_country STRING, 
    album_type STRING, played_ms BIGINT, played_min STRING, played_h STRING, bpm INT
) PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/spotify/warehouse/fact_streams';

-- Populate the fact table by driving YARN/Tez to pipeline data into corresponding year/month nodes
INSERT OVERWRITE TABLE fact_streams_detail PARTITION(year, month)
SELECT end_time, end_day, end_hour_utc, end_hour_pst, artist_name, track_name, album_name, release_year, release_country, album_type, played_ms, played_min, played_h, bpm, end_year, end_month 
FROM tmp_staging WHERE end_year IS NOT NULL;

-- Reverse-engineer and generate the daily snapshot dimension table (Replaces counts_day)
CREATE TABLE dim_time_counts AS
SELECT 
    year AS end_year, month AS end_month, CAST(SUBSTRING(end_time, 4, 2) AS INT) AS end_date, end_day, end_hour_utc, end_hour_pst,
    COUNT(1) AS number_of_streams, SUM(played_ms) AS total_time_played_ms,
    ROUND(SUM(played_ms) / 3600000.0, 2) AS total_time_played_h,
    ROUND(AVG(played_ms), 2) AS average_ms_played, ROUND(AVG(played_ms) / 60000.0, 2) AS average_min_played
FROM fact_streams_detail GROUP BY year, month, SUBSTRING(end_time, 4, 2), end_day, end_hour_utc, end_hour_pst;

-- Reverse-engineer and generate the track and album metric dimension table (Replaces counts_album)
CREATE TABLE dim_albums AS
SELECT 
    release_year, album_type, album_name, track_name, bpm,
    COUNT(1) AS number_of_streams, ROUND(SUM(played_ms) / 60000.0, 2) AS total_time_played,
    ROUND(SUM(played_ms) / 3600000.0, 2) AS total_time_played_h, AVG(played_ms) AS average_ms_played,
    ROUND(AVG(played_ms) / 60000.0, 2) AS average_ms_played_min, '3:30' AS song_length_min,
    ROUND(AVG(CASE WHEN played_ms >= 180000 THEN 100.0 ELSE (played_ms / 180000.0) * 100.0 END), 2) AS avg_percent_song_played
FROM fact_streams_detail GROUP BY release_year, album_type, album_name, track_name, bpm;

-- Verification: row counts for both dimension tables
SELECT 'dim_albums' AS table_verification, COUNT(1) AS total_records FROM skz_spotify_db.dim_albums
UNION ALL
SELECT 'dim_time_counts', COUNT(1) FROM skz_spotify_db.dim_time_counts;
