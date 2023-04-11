/**
  README:
  1. Go to the directory where we store the tsv files
  2. Run cmd hadoop fs -mkdir /hive_data to create a directory called
     hive_data in HDFS
  3. Run cmd hadoop fs -put *.tsv /hive_data to load all tscv files to
     hive_data in HDFS
  4. Run cmd hadoop fs -ls /hive_data to list all files in hive_data
     for verification.
  5. If all files are loaded successfully, run HiveSQL cmd below to
     create tables and load data into tables.
  */

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
LOCATION '/hive_data'
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA INPATH '/hive_data/title.akas.tsv' INTO TABLE akas;

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
LOCATION '/hive_data'
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA INPATH '/hive_data/title.basics.tsv' INTO TABLE basics;


-- load data from title.ratings --> ratings
CREATE TABLE IF NOT EXISTS ratings (
  titleId STRING,
  rating DOUBLE,
  numVotes INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '/hive_data'
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA INPATH '/hive_data/title.ratings.tsv' INTO TABLE ratings;


-- load data from title.crew --> crew
CREATE TABLE IF NOT EXISTS crew (
  titleId STRING,
  directors ARRAY<STRING>,
  writers ARRAY<STRING>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/hive_data'
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA INPATH '/hive_data/title.crew.tsv' INTO TABLE crew;


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
LOCATION '/hive_data'
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA INPATH '/hive_data/name.basics.tsv' INTO TABLE names;