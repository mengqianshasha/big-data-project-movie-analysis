USE movie;

-- The output data of this query is the top 3 rated genres of each country.
-- The output data is stored in the top_genre table.

-- OUTPUT DATA SAMPLE:
-- +----------------+--------------+--------------------+----------+
-- |    country     |    genre     | weightedaverating  | ranking  |
-- +----------------+--------------+--------------------+----------+
-- | China          | Romance      | 6.5                | 1        |
-- | China          | Animation    | 6.5                | 1        |
-- | China          | Documentary  | 5.90814757878555   | 3        |
-- | India          | Animation    | 5.6                | 1        |
-- | Japan          | Romance      | 6.2                | 1        |
-- | Japan          | Documentary  | 5.8                | 2        |
-- | Japan          | Animation    | 5.8                | 2        |
-- | United States  | Documentary  | 5.7                | 1        |
-- | United States  | Animation    | 5.6                | 2        |
-- +----------------+--------------+--------------------+----------+


-- Create a table for the result
CREATE TABLE IF NOT EXISTS top_genre (
	country STRING,
	genre STRING,
	weightedAveRating DOUBLE,
	ranking INT
);

CREATE VIEW top_genre_temp AS
WITH full_table AS (
SELECT
	b.titleId,
	b.titleType,
	b.startYear,
	b.genres,
	c.country,
	r.rating,
	r.numVotes
FROM basics b
JOIN countries c ON b.titleId=c.titleId
JOIN ratings r ON b.titleId=r.titleId
WHERE 
	b.startYear>=2010
	AND b.titleType='movie'
	AND b.titleId IS NOT NULL
    AND b.startYear IS NOT NULL 
    AND b.genres IS NOT NULL
    AND c.country IS NOT NULL
    AND r.rating IS NOT NULL 
    AND r.numVotes IS NOT NULL
),
exploded_table AS (
SELECT 
	f.titleId,
	f.rating,
	f.numVotes,
	genre_exploded.genre,
	country_exploded.country
FROM
	full_table f
	LATERAL VIEW EXPLODE(f.genres) genre_exploded AS genre
	LATERAL VIEW EXPLODE(f.country) country_exploded AS country
WHERE genre_exploded.genre!="movie"
),
weighted_avg_scores AS (
SELECT
	ex.country,
	ex.genre,
	SUM(ex.rating * ex.numVotes) / SUM(ex.numVotes) AS weightedAveRating
FROM
	exploded_table ex
GROUP BY
	ex.country,
	ex.genre
),
ranked_scores AS (
    SELECT
        country,
        genre,
        weightedAveRating,
        RANK() OVER (PARTITION BY country ORDER BY weightedAveRating DESC) AS ranking
    FROM
        weighted_avg_scores
)
SELECT 
    country,
    genre,
    weightedAveRating,
    ranking
FROM ranked_scores
ORDER BY
    country,
    weightedAveRating DESC;

INSERT OVERWRITE TABLE top_genre
SELECT * FROM top_genre_temp;

DROP VIEW top_genre_temp;
