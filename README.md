#Processing Movielens data using Apache Spark

The dataset for this lab was downloaded from [the datasets page in the movielens website](http://files.grouplens.org/datasets/movielens/ml-latest.zip)

The project processes the datasets and creates output files (parquet or textfiles) that can be loaded into Hive or Impala tables for analysis.

The project contains two main classes 
- **MovieRatingsApp** -	creates outputs to analyse, Movies, Ratings, MovieRatings (aggregation)
- **MovieTagsApp**  - creates output to analyse tags, relationships between movie and tags, tags  and movies, tag and ratings. 


***

##Building the project
To build this project, ensure you have scala and sbt installed in your machine, cd into the working directory, and type
	sbt clean package

This will create the folder path - *target/scala-2.10/movielens-latest_2.10-1.0.jar*, in the current directory 


##Running the application
There are two format options for each class
- Text data format
- Parquet data format

See the options:
#####Running MovieRatingApp for output in TextFormat
	spark-submit --master local[*] --class MovieRatingsApp ./target/scala-2.10/movielens-latest_2.10-1.0.jar hdfs://localhost:9000/user/hadmin/rawdata/movielens/latest hdfs://localhost:9000/user/hadmin/solution/movielens/lt/txt text

#####Running MovieTagsApp for output in TextFormat
	spark-submit --master local[*] --class MovieTagsApp ./target/scala-2.10/movielens-latest_2.10-1.0.jar hdfs://localhost:9000/user/hadmin/rawdata/movielens/latest hdfs://localhost:9000/user/hadmin/solution/movielens/lt/txt text

#####Running MovieRatingsApp for output in parquet format
spark-submit --master local[*] --class MovieRatingsApp ,/target/scala-2.10/movielens-latest_2.10-1.0.jar hdfs://localhost:9000/user/hadmin/rawdata/movielens/latest hdfs://localhost:9000/user/hadmin/solution/movielens/lt/pakey parquet

#####Running MovieTagsApp for output in parquet format
spark-submit --master local[*] --class MovieTagsApp ./target/scala-2.10/movielens-latest_2.10-1.0.jar hdfs://localhost:9000/user/hadmin/rawdata/movielens/latest hdfs://localhost:9000/user/hadmin/solution/movielens/lt/pakey parquet
