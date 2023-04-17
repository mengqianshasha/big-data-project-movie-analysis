-- This Hive SQL script combines the results of the top_genre and best_director tables
-- and stores the merged data in a new table called 'best_director_for_genres'.
-- This new table displays the top 3 genres for each country along with their corresponding best directors.

-- Note:
-- - Some rows have duplicate 'ranking' values due to ties.
-- - The 'best_directors' column in the new table is an array since some directors have same rating score.


-- Result sample:
-- -----------------------------------+---------------------------------+-----------------------------------+----------------------------------------------------+
-- | best_director_for_genres.country  | best_director_for_genres.genre  | best_director_for_genres.ranking  |      best_director_for_genres.best_directors       |
-- -----------------------------------+---------------------------------+-----------------------------------+----------------------------------------------------+
-- | Afghanistan                       | Music                           | 1                                 | ["Cary Stuart"]                                    |
-- | Afghanistan                       | Adventure                       | 2                                 | ["Yama Rahimi"]                                    |
-- | Afghanistan                       | War                             | 3                                 | ["Ed Howson"]                                      |
-- | Albania                           | Mystery                         | 1                                 | ["Jotti Ejlli"]                                    |
-- | Albania                           | Documentary                     | 2                                 | ["Toma Enache"]                                    |
-- | Albania                           | Thriller                        | 3                                 | ["Jotti Ejlli"]                                    |
-- | American Samoa                    | Family                          | 1                                 | ["Tony Vainuku","Erika Cohn"]                      |
-- | American Samoa                    | Documentary                     | 1                                 | ["Tony Vainuku","Erika Cohn"]                      |
-- | American Samoa                    | Sport                           | 1                                 | ["Erika Cohn","Tony Vainuku"]                      |
-- -----------------------------------+---------------------------------+-----------------------------------+----------------------------------------------------+



CREATE TABLE IF NOT EXISTS best_director_for_genres (
	country STRING,
	genre STRING,
	ranking INT,
	best_directors ARRAY<STRING>
);

DROP VIEW IF EXISTS result_temp;
CREATE VIEW result_temp AS
WITH combined_data AS (
SELECT
	g.country,
	g.genre,
	g.ranking AS original_ranking,
	COLLECT_LIST(d.director_name) AS best_directors
FROM
	top_genre g
JOIN
	best_director d ON g.country = d.country AND g.genre = d.genre
GROUP BY 
	g.country,
	g.genre,
	g.ranking
),
ranked_data AS (
    SELECT
        country,
        genre,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY original_ranking) AS ranking,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY original_ranking) AS rowN,
        best_directors
    FROM
        combined_data
)
SELECT 
    country,
    genre,
    ranking,
    best_directors
FROM ranked_data
WHERE rowN <= 3
ORDER BY country, ranking;

INSERT OVERWRITE TABLE best_director_for_genres
SELECT * FROM result_temp;

DROP VIEW result_temp;