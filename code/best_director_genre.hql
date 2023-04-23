CREATE DATABASE IF NOT EXISTS movie;
USE movie;


-- load data from title.basics --> basics
CREATE TABLE IF NOT EXISTS basics (
  titleId STRING,
  titleType STRING,
  primaryTitle STRING,
  originalTitle STRING,
  isAdult INT,
  startYear INT,
  endYear INT,
  runtimeMinutes INT,
  genres ARRAY<STRING>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ('skip.header.line.count'='1');


-- load data from title.ratings --> ratings
CREATE TABLE IF NOT EXISTS ratings (
  titleId STRING,
  rating DOUBLE,
  numVotes INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
TBLPROPERTIES ('skip.header.line.count'='1');


-- load data from title.crew --> crew
CREATE TABLE IF NOT EXISTS crew (
  titleId STRING,
  directors ARRAY<STRING>,
  writers ARRAY<STRING>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ('skip.header.line.count'='1');


-- load data from name.basics -> names
CREATE TABLE IF NOT EXISTS names (
  nameId STRING,
  primaryName STRING,
  birthYear INT,
  deathYear INT,
  primaryProfession ARRAY<STRING>,
  knownForTitles ARRAY<STRING>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ('skip.header.line.count'='1');



-- load data from title.country -> countries
CREATE TABLE IF NOT EXISTS countries (
  titleId STRING,
  country ARRAY<STRING>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
LINES TERMINATED BY '\n';



LOAD DATA INPATH 's3://liuni-6240/movie-data/title.basics.tsv' OVERWRITE INTO TABLE basics;
LOAD DATA INPATH 's3://liuni-6240/movie-data/title.ratings.tsv' OVERWRITE INTO TABLE ratings;
LOAD DATA INPATH 's3://liuni-6240/movie-data/title.crew.tsv' OVERWRITE INTO TABLE crew;
LOAD DATA INPATH 's3://liuni-6240/movie-data/name.basics.tsv' OVERWRITE INTO TABLE names;
LOAD DATA INPATH 's3://liuni-6240/movie-data/title.country.tsv' OVERWRITE INTO TABLE countries;


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


CREATE TABLE IF NOT EXISTS best_director (
    country STRING,
    genre STRING,
    director_name STRING,
    weighted_rating DOUBLE
);

CREATE VIEW best_director_tmp AS
WITH all_info AS (
    SELECT
        c.country AS countries,
        b.genres AS genres,
        cr.directors AS directors,
        b.titleId AS title_id,
        r.rating AS rating,
        r.numVotes AS numvotes
    FROM
        basics b
        JOIN countries c ON b.titleId = c.titleId
        JOIN crew cr ON b.titleId = cr.titleId
        JOIN ratings r ON b.titleId = r.titleId
    WHERE
        b.titleType = 'movie' AND 
        b.startYear >= 2010 AND
        b.titleId IS NOT NULL AND
        b.startYear IS NOT NULL AND
        b.genres IS NOT NULL AND
        c.titleId IS NOT NULL AND
        c.country IS NOT NULL AND
        r.titleId IS NOT NULL AND
        r.rating IS NOT NULL AND
        r.numVotes IS NOT NULL AND
        cr.titleId IS NOT NULL AND
        size(cr.directors) > 0
),
exploded_all_info AS (
    SELECT
        countries_.country AS country,
        genres_.genre AS genre,
        directorts_.director AS director,
        title_id,
        rating,
        numvotes
    from all_info
        LATERAL VIEW EXPLODE(countries) countries_ AS country
        LATERAL VIEW EXPLODE(genres) genres_ AS genre
        LATERAL VIEW EXPLODE(directors) directorts_ AS director
    where
        genres_.genre != "movie"
),
weighted_rating_agg AS (
    SELECT
        country,
        genre,
        director,
        ROUND(SUM(rating * numvotes) / SUM(numvotes), 1) AS weighted_rating
    FROM
        exploded_all_info
    GROUP BY
        country,
        genre,
        director
),
director_rank AS (
    SELECT
        country,
        genre,
        director,
        weighted_rating,
        RANK() OVER (PARTITION BY country, genre ORDER BY weighted_rating DESC) AS rank
    FROM
        weighted_rating_agg
)
select
    d.country as country,
    d.genre as genre,
    n.primaryName as director_name,
    weighted_rating as rating
from director_rank d join names n on d.director = n.nameId
where rank = 1
order by country, genre;


INSERT OVERWRITE TABLE best_director
SELECT * FROM best_director_tmp;

DROP VIEW best_director_tmp;


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