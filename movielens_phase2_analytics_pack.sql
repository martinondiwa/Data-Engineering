-- Spark SQL reference script for the same analytics pack in our Colab notebook.
-- This is more of a reference .sql file, because the notebook handles actual analytics.
CREATE DATABASE IF NOT EXISTS phase2_db;
USE phase2_db;

-- 1 Highest-rated movies with at least 1k ratings
SELECT
    m.movie_id,
    m.title,
    COUNT(*) AS rating_count,
    ROUND(AVG(r.rating), 3) AS avg_rating
FROM ratings r
JOIN movies m ON r.movie_id = m.movie_id
GROUP BY m.movie_id, m.title
HAVING COUNT(*) >= 1000
ORDER BY avg_rating DESC, rating_count DESC
LIMIT 20;

-- 2 Genre-level rating volume and average score
SELECT
    genre,
    COUNT(*) AS rating_events,
    ROUND(AVG(r.rating), 3) AS avg_rating
FROM ratings r
JOIN movies m ON r.movie_id = m.movie_id
LATERAL VIEW explode(split(m.genres, '\\|')) genre_view AS genre
GROUP BY genre
HAVING COUNT(*) >= 5000
ORDER BY rating_events DESC, avg_rating DESC
LIMIT 20;

-- 3 Rating performance by release era
SELECT
    era,
    COUNT(DISTINCT movie_id) AS movie_count,
    ROUND(AVG(rating), 3) AS avg_rating
FROM (
    SELECT
        m.movie_id,
        r.rating,
        CASE
            WHEN m.release_year IS NULL THEN 'Unknown'
            WHEN m.release_year < 1970 THEN 'Before 1970'
            WHEN m.release_year BETWEEN 1970 AND 1979 THEN '1970s'
            WHEN m.release_year BETWEEN 1980 AND 1989 THEN '1980s'
            WHEN m.release_year BETWEEN 1990 AND 1999 THEN '1990s'
            WHEN m.release_year BETWEEN 2000 AND 2009 THEN '2000s'
            WHEN m.release_year BETWEEN 2010 AND 2019 THEN '2010s'
            ELSE '2020s and later'
        END AS era
    FROM movies m
    LEFT JOIN ratings r ON m.movie_id = r.movie_id
) era_stats
GROUP BY era
ORDER BY movie_count DESC;

-- 4 Movies above the overall average rating
SELECT
    movie_id,
    title,
    ROUND(avg_rating, 3) AS avg_rating,
    rating_count
FROM (
    SELECT
        m.movie_id,
        m.title,
        AVG(r.rating) AS avg_rating,
        COUNT(*) AS rating_count
    FROM movies m
    JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title
) movie_stats
WHERE avg_rating > (SELECT AVG(rating) FROM ratings)
  AND rating_count >= 500
ORDER BY avg_rating DESC, rating_count DESC
LIMIT 20;

-- 5 Top 5 movies inside each genre by average rating
SELECT
    genre,
    title,
    avg_rating,
    rating_count,
    genre_rank
FROM (
    SELECT
        genre,
        title,
        avg_rating,
        rating_count,
        DENSE_RANK() OVER (
            PARTITION BY genre
            ORDER BY avg_rating DESC, rating_count DESC, title ASC
        ) AS genre_rank
    FROM (
        SELECT
            genre,
            m.title AS title,
            ROUND(AVG(r.rating), 3) AS avg_rating,
            COUNT(*) AS rating_count
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        LATERAL VIEW explode(split(m.genres, '\\|')) genre_view AS genre
        GROUP BY genre, m.title
        HAVING COUNT(*) >= 500
    ) genre_movie_stats
) ranked_genre_movies
WHERE genre_rank <= 5
ORDER BY genre, genre_rank, rating_count DESC;

-- 6 Tags associated with highly rated user-movie interactions
SELECT
    t.tag,
    COUNT(*) AS tag_uses,
    ROUND(AVG(r.rating), 3) AS avg_rating_on_tagged_movies
FROM tags t
JOIN ratings r
    ON t.user_id = r.user_id
   AND t.movie_id = r.movie_id
JOIN movies m ON t.movie_id = m.movie_id
GROUP BY t.tag
HAVING COUNT(*) >= 50
ORDER BY avg_rating_on_tagged_movies DESC, tag_uses DESC
LIMIT 20;

-- 7 Most-tagged movies and their average ratings
SELECT
    m.title,
    tag_stats.tag_count,
    rating_stats.avg_rating,
    rating_stats.unique_raters
FROM movies m
JOIN (
    SELECT movie_id, COUNT(*) AS tag_count
    FROM tags
    GROUP BY movie_id
    HAVING COUNT(*) >= 100
) tag_stats ON m.movie_id = tag_stats.movie_id
LEFT JOIN (
    SELECT
        movie_id,
        ROUND(AVG(rating), 3) AS avg_rating,
        COUNT(DISTINCT user_id) AS unique_raters
    FROM ratings
    GROUP BY movie_id
) rating_stats ON m.movie_id = rating_stats.movie_id
ORDER BY tag_stats.tag_count DESC, rating_stats.avg_rating DESC
LIMIT 20;

-- 8 User engagement bands based on rating activity
SELECT
    activity_band,
    COUNT(*) AS user_count,
    ROUND(AVG(avg_user_rating), 3) AS avg_of_user_avg_ratings,
    ROUND(AVG(total_ratings), 1) AS avg_ratings_per_user
FROM (
    SELECT
        user_id,
        COUNT(*) AS total_ratings,
        AVG(rating) AS avg_user_rating,
        CASE
            WHEN COUNT(*) < 100 THEN 'Casual'
            WHEN COUNT(*) < 500 THEN 'Engaged'
            WHEN COUNT(*) < 1000 THEN 'Power'
            ELSE 'Super'
        END AS activity_band
    FROM ratings
    GROUP BY user_id
) user_stats
GROUP BY activity_band
ORDER BY avg_ratings_per_user DESC, user_count DESC;
