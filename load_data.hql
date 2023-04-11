CREATE DATABASE IF NOT EXISTS movie;
USE movie;

-- load data from title.akas --> akas
CREATE TABLE IF NOT EXISTS akas (
  titleId STRING,
  ordering INT,
  title STRING,
  region STRING,
  language STRING,
  types STRING,
  attributes STRING,
  isOriginalTitle INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA LOCAL INPATH '/Users/qianshameng/Documents/Academic/NEU/Course/6240_big_data/project/data/imdb/sample/title.akas.tsv' INTO TABLE akas;


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

LOAD DATA LOCAL INPATH '/Users/qianshameng/Documents/Academic/NEU/Course/6240_big_data/project/data/imdb/sample/title.basics.tsv' INTO TABLE basics;


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

LOAD DATA LOCAL INPATH '/Users/qianshameng/Documents/Academic/NEU/Course/6240_big_data/project/data/imdb/sample/title.ratings.tsv' INTO TABLE ratings;


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

LOAD DATA LOCAL INPATH '/Users/qianshameng/Documents/Academic/NEU/Course/6240_big_data/project/data/imdb/sample/title.crew.tsv' INTO TABLE crew;


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

LOAD DATA LOCAL INPATH '/Users/qianshameng/Documents/Academic/NEU/Course/6240_big_data/project/data/imdb/sample/name.basics.tsv' INTO TABLE names;


-- load data from title.country -> country
CREATE TABLE IF NOT EXISTS countries (
  titleId STRING,
  country ARRAY<STRING>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '/Users/qianshameng/Documents/Academic/NEU/Course/6240_big_data/project/data/imdb/sample/title.country.tsv' INTO TABLE countries;











