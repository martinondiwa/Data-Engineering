-- Spark SQL reference script for the same table build in our Colab notebook.
-- This is more of a reference .sql file, because the notebook handles actual
-- data loading and transformation.

-- -- -- -- -- -- -- START TABLE CREATION -- -- -- -- -- -- --
CREATE DATABASE IF NOT EXISTS phase2_db;
USE phase2_db;

DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS links;
DROP TABLE IF EXISTS movies;

-- Spark does not enforce PK/FK constraints, so these keys are documentation only.
-- Columns are marked as PK/FK with comments as if they were in a true SQL DB.
CREATE TABLE movies (
    -- PK: movie_id
    movie_id INT NOT NULL,
    title STRING NOT NULL,
    genres STRING NOT NULL,
    release_year INT
) USING CSV;

CREATE TABLE ratings (
    -- composite PK: (user_id, movie_id)
    -- FK: movie_id
    user_id BIGINT NOT NULL,
    movie_id INT NOT NULL,
    rating DOUBLE NOT NULL,
    rating_timestamp TIMESTAMP NOT NULL
) USING CSV;

CREATE TABLE tags (
    -- composite PK: (user_id, movie_id, tag_timestamp)
    -- FK: movie_id
    user_id BIGINT NOT NULL,
    movie_id INT NOT NULL,
    tag STRING,
    tag_timestamp TIMESTAMP NOT NULL
) USING CSV;

CREATE TABLE links (
    -- PK: movie_id
    -- FK: movie_id
    movie_id INT NOT NULL,
    imdb_id BIGINT,
    tmdb_id BIGINT
) USING CSV;

-- Insert data from staging tables
INSERT INTO movies
SELECT movie_id, title, genres, release_year
FROM movies_staging;

INSERT INTO ratings
SELECT user_id, movie_id, rating, rating_timestamp
FROM ratings_staging;

INSERT INTO tags
SELECT user_id, movie_id, tag, tag_timestamp
FROM tags_staging;

INSERT INTO links
SELECT movie_id, imdb_id, tmdb_id
FROM links_staging;

-- -- -- -- -- -- -- START VERIFICATION PACK -- -- -- -- -- -- --
-- Verification pack: confirm each table loaded the expected number of rows.
SELECT 'movies' AS table_name, COUNT(*) AS row_count FROM movies
UNION ALL
SELECT 'ratings' AS table_name, COUNT(*) AS row_count FROM ratings
UNION ALL
SELECT 'tags' AS table_name, COUNT(*) AS row_count FROM tags
UNION ALL
SELECT 'links' AS table_name, COUNT(*) AS row_count FROM links
ORDER BY table_name;

-- Data quality audit: Check for suspicious or impossible values
SELECT 'ratings_out_of_range' AS issue, COUNT(*) AS record_count
FROM ratings
WHERE rating < 0.5 OR rating > 5.0
UNION ALL
SELECT 'ratings_missing_movie' AS issue, COUNT(*) AS record_count
FROM ratings r
LEFT JOIN movies m ON r.movie_id = m.movie_id
WHERE m.movie_id IS NULL
UNION ALL
SELECT 'blank_tags' AS issue, COUNT(*) AS record_count
FROM tags
WHERE tag IS NULL OR TRIM(tag) = ''
UNION ALL
SELECT 'future_timestamps' AS issue, COUNT(*) AS record_count
FROM (
    SELECT rating_timestamp AS event_timestamp FROM ratings
    UNION ALL
    SELECT tag_timestamp AS event_timestamp FROM tags
) audit_events
WHERE event_timestamp > current_timestamp()
UNION ALL
SELECT 'negative_link_ids' AS issue, COUNT(*) AS record_count
FROM links
WHERE COALESCE(imdb_id, 0) < 0 OR COALESCE(tmdb_id, 0) < 0
ORDER BY issue;
