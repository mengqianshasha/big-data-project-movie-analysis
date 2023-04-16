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

WITH exploded_countries AS (
    SELECT
        titleId,
        single_country
    FROM
        countries
        LATERAL VIEW EXPLODE(country) country_ AS single_country
),
exploded_crew AS (
    SELECT
        titleId,
        single_director
    FROM
        crew
        LATERAL VIEW EXPLODE(directors) directors_ AS single_director
),
exploded_basics AS (
    SELECT
        titleId,
        single_genre
    FROM
        basics
        LATERAL VIEW EXPLODE(genres) genres_ AS single_genre
),
all_info AS (
    SELECT
        c.single_country AS country,
        b.single_genre AS genre,
        cr.single_director AS director,
        n.primaryName AS director_name,
        b.titleId AS title_id,
        r.rating AS rating,
        r.numVotes AS numvotes
    FROM
        exploded_basics b
        JOIN exploded_countries c ON b.titleId = c.titleId
        JOIN exploded_crew cr ON b.titleId = cr.titleId
        JOIN ratings r ON b.titleId = r.titleId
        JOIN names n ON cr.single_director = n.nameId
),
weighted_rating_agg AS (
    SELECT
        country,
        genre,
        director,
        director_name,
        SUM(rating * numvotes) / SUM(numvotes) AS weighted_rating
    FROM
        all_info
    GROUP BY
        country,
        genre,
        director,
        director_name
),
director_rank AS (
    SELECT
        country,
        genre,
        director_name,
        weighted_rating,
        RANK() OVER (PARTITION BY country, genre ORDER BY weighted_rating DESC) AS rank
    FROM
        weighted_rating_agg
)

SELECT
    country,
    genre,
    director_name,
    weighted_rating
FROM
    director_rank
WHERE
    rank = 1
ORDER BY
    country,
    genre;