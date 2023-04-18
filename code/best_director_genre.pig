-- Load data from files
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
DEFINE CSVLoaderWithHeader org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

%declare DIR '/Users/qianshameng/Documents/Academic/NEU/Course/6240_big_data/project/data/imdb/full_movies';
basics = LOAD '$DIR/title.basics.tsv' USING CSVLoaderWithHeader('\t') AS (titleId:chararray, titleType:chararray, primaryTitle:chararray, originalTitle:chararray, isAdult:int, startYear:int, endYear:int, runtimeMinutes:int, genres:chararray);
ratings = LOAD '$DIR/title.ratings.tsv' USING CSVLoaderWithHeader('\t') AS (titleId:chararray, rating:chararray, numVotes:chararray);
crew = LOAD '$DIR/title.crew.tsv' USING CSVLoaderWithHeader('\t') AS (titleId:chararray, directors:chararray, writers:chararray);
names = LOAD '$DIR/name.basics.tsv' USING CSVLoaderWithHeader('\t') AS (nameId:chararray, primaryName:chararray, birthYear:int, deathYear:int, primaryProfession:chararray, knownForTitles:chararray);
countries = LOAD '$DIR/title.country.tsv' USING CSVLoaderWithHeader('\t') AS (titleId:chararray, country:chararray);

-- Filter and Join tables
basics = FILTER basics BY 
	titleType == 'movie' AND 
	startYear >= 2010 AND
	titleId IS NOT NULL AND titleId != '\\N' AND
	genres IS NOT NULL AND genres != '\\N';
countries = FILTER countries BY titleId != '\N' AND country != '\\N';
crew = FILTER crew BY titleId != '\\N' AND directors != '\\N';
names = FILTER names BY nameId != '\\N' AND primaryName != '\\N';
ratings = FILTER ratings BY titleId != '\\N' AND rating != '\N' AND numVotes != '\N';
ratings = FOREACH ratings GENERATE
	titleId AS titleId,
	(DOUBLE)rating AS rating,
	(DOUBLE) numVotes AS numVotes;
basics_ratings = JOIN basics BY titleId, ratings BY titleId;
basics_ratings = FOREACH basics_ratings GENERATE 
	basics::titleId AS titleId,
	basics::genres AS genres,
	ratings::rating AS rating,
	ratings::numVotes AS numVotes;

basics_ratings_countries = JOIN basics_ratings BY titleId, countries BY titleId;

all_data = JOIN basics_ratings_countries BY basics_ratings::titleId, crew BY titleId;
all_data = FOREACH all_data GENERATE 
	basics_ratings_countries::basics_ratings::titleId AS titleId, 
	basics_ratings_countries::basics_ratings::genres AS genres, 
	basics_ratings_countries::basics_ratings::rating AS rating, 
	basics_ratings_countries::basics_ratings::numVotes AS numVotes, 
	basics_ratings_countries::countries::country AS countries, 
	crew::directors AS directors;
exploded_table = FOREACH all_data GENERATE 
	titleId, 
	FLATTEN(TOKENIZE(genres, ',')) AS genre,
	rating,
	numVotes,
	FLATTEN(TOKENIZE(countries, ',')) AS country,
	directors;

-- Generate data for top_genre
grouped_data = GROUP exploded_table BY (country, genre);
top_genre_agg = FOREACH grouped_data {
    rating_times_votes = FOREACH exploded_table GENERATE rating * numVotes as rating_times_votes;
    total_rating_times_votes = SUM(rating_times_votes.rating_times_votes);
    total_votes = SUM(exploded_table.numVotes);
    weightedAveRating = total_rating_times_votes / (DOUBLE)total_votes;
    GENERATE group.country AS country, group.genre AS genre, weightedAveRating AS weightedAveRating;
};
top_genre_agg = ORDER top_genre_agg BY country, weightedAveRating DESC;

-- Generate data for best_director
directors_exploded = FOREACH exploded_table GENERATE 
	titleId AS titleId,
	country AS country, 
	genre AS genre, 	 
	FLATTEN(TOKENIZE(directors, ',')) AS flattened_director, 
	rating AS rating, 
	numVotes AS numVotes;
director_grouped = GROUP directors_exploded BY (country, genre, flattened_director);
weighted_average = FOREACH director_grouped {
    rating_times_votes = FOREACH directors_exploded GENERATE rating * numVotes AS rating_times_votes;
    total_rating_times_votes = SUM(rating_times_votes.rating_times_votes);
    total_votes = SUM(directors_exploded.numVotes);
    weightedAveRating = total_rating_times_votes / (DOUBLE)total_votes;
    GENERATE group.country AS country, group.genre AS genre, group.flattened_director AS director, weightedAveRating AS weightedAveRating;
};

weighted_average_named_director = JOIN weighted_average BY director, names BY nameId;
weighted_average_named_director = FOREACH weighted_average_named_director GENERATE
	weighted_average::country AS country,
	weighted_average::genre AS genre,
	weighted_average::director AS director,
	names::primaryName AS primaryName,
	weighted_average::weightedAveRating AS weightedAveRating;
weighted_average_named_director = FILTER weighted_average_named_director BY primaryName != '\N';

-- Keep top directors who have the max weightedAveRating
grouped_director = GROUP weighted_average_named_director BY (country, genre);
top_directors_rating = FOREACH grouped_director {
    maxWeightedAveRating = MAX(weighted_average_named_director.weightedAveRating);
    GENERATE
        group.country AS country,
        group.genre AS genre,
        maxWeightedAveRating AS weightedAveRating;
};
top_directors = JOIN weighted_average_named_director BY (country, genre, weightedAveRating), top_directors_rating BY (country, genre, weightedAveRating);
top_directors = FOREACH top_directors GENERATE
	weighted_average_named_director::country AS country,
	weighted_average_named_director::genre AS genre,
	weighted_average_named_director::primaryName AS director;

grouped_directors = GROUP top_directors BY (country, genre);
director_bags = FOREACH grouped_directors GENERATE FLATTEN(group) AS (country, genre), top_directors.director AS directors;

-- Merge top directors and top genres
joined_data = JOIN top_genre_agg BY (country, genre), director_bags BY (country, genre);
joined_data = FOREACH joined_data GENERATE
	top_genre_agg::country AS country,
	top_genre_agg::genre AS genre,
	top_genre_agg::weightedAveRating AS weightedAveRating,
	director_bags::directors AS topDirectors;
grouped_data = GROUP joined_data BY country;

top_2_rows = FOREACH grouped_data {
    sorted_data = ORDER joined_data BY weightedAveRating DESC;
    top_2 = LIMIT sorted_data 3;
    GENERATE FLATTEN(top_2);
}

top_2_rows = ORDER top_2_rows BY top_2::country, top_2::weightedAveRating DESC;


STORE top_2_rows INTO '$DIR/output/final2' USING PigStorage('\t');


