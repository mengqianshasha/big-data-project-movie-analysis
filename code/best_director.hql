/**
  This Hive SQL script will get the best director of each genre in each country based on
  the ratings.

  Sample output:

  +-----------------------------------+--------------+------------------------------+---------------------+
  |             country               |    genre     |        director_name         |   weighted_rating   |
  +-----------------------------------+--------------+------------------------------+---------------------+
  | United Kingdom                    | Reality-TV   | Josh Schultz                 | 5.700000000000001   |
  | United Kingdom                    | Reality-TV   | Dane Keil                    | 5.700000000000001   |
  | United Kingdom                    | Romance      | Olivier Adam                 | 9.2                 |
  | United Kingdom                    | Sci-Fi       | Sandra Daroy                 | 8.5                 |
  | United Kingdom                    | Sport        | Jack Turner                  | 10.0                |
  | United Kingdom                    | Thriller     | Simon Ellis                  | 9.2                 |
  | United Kingdom                    | War          | Pradeep Shahi                | 9.3                 |
  | United Kingdom                    | Western      | Luke J. Hagan                | 7.700000000000001   |
  | United States                     | Action       | Lance Larson                 | 9.8                 |
  | United States                     | Adult        | Huggy Bear                   | 9.1                 |
  | United States                     | Adventure    | Joel Newton                  | 9.7                 |
  | United States                     | Animation    | Christopher DeMaci           | 9.5                 |
  | United States                     | Biography    | Vasovic Danko                | 9.8                 |
  | United States                     | Comedy       | Scott Peters                 | 9.9                 |
  | United States                     | Crime        | E.C Illa                     | 10.0                |
  +-----------------------------------+--------------+------------------------------+---------------------+

 */

USE movie;

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

