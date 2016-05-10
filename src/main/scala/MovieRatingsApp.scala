import com.okmich.spark.mllatest.model._

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object MovieRatingsApp {

	def main(args : Array[String]) : Unit = {
		if (args.length != 3){
			println("No arguments!!! Use <inputPath> <outputFolderPath> <format>")
			return; 
		}
		val input_path = args(0)
		val output_path = args(1)

		val sc = getSparkContext()

		val movieSourceRdd = sc.textFile(input_path + "/movies")
		val ratingSourceRdd = sc.textFile(input_path + "/ratings")
		//remove headers
		val movieFilteredRdd = returnRemovedHeader(movieSourceRdd, "movieId")
		val ratingFilteredRdd = returnRemovedHeader(ratingSourceRdd, "userId")

		//map each line to objects
		val moviesRDD = movieFilteredRdd map (MovieCreator.lineToMovie(_))
		val ratingsRDD = ratingFilteredRdd map (RatingCreator.lineToRating(_))
		//create a pair rdd for movie
		val moviePairRdd = moviesRDD map (movie => (movie.id, movie))
		val ratingPairRdd = ratingsRDD map (rating => (rating.movieId, rating))
		//persists each cache
		moviePairRdd.persist(StorageLevel.MEMORY_AND_DISK)
		ratingPairRdd.persist(StorageLevel.MEMORY_AND_DISK)


		//create the movie rating dataset
		val movieRatingRdd = getMovieRating(moviePairRdd, ratingPairRdd)


		val format = args(2)
		if (format.toLowerCase == "text"){
			//output rdd to disk as text file
			moviePairRdd.values.saveAsTextFile(output_path + "/movies")
			ratingPairRdd.values.saveAsTextFile(output_path + "/ratings")
			movieRatingRdd.saveAsTextFile(output_path + "/movie_ratings")
		} else if (format.toLowerCase == "parquet"){
			val sqlContext = new SQLContext(sc)
			import sqlContext.implicits._

			//output rdd to disk as parquet
			val newMovieRDD = moviesRDD.toDF()
			newMovieRDD.write.parquet(output_path + "/parquet/movies")
			ratingPairRdd.values.toDF().write.parquet(output_path + "/parquet/rating")
			movieRatingRdd.toDF().write.parquet(output_path + "/parquet/movie_ratings")
		} else {
			println("Invalid write format")
		}
	}

	def getMovieRating(movieRdd: RDD[(Int, Movie)], ratingPairRdd: RDD[(Int, Rating)]): RDD[MovieRating] = {
		import scala.math.{min, max}

		val combinedMovieRating = ratingPairRdd combineByKey(
			(r: Rating) => new MovieRating(r.movieId, "",  0, r.rating, 1, r.timestamp, r.timestamp),
			(m: MovieRating, r: Rating) => new MovieRating(m.movieId, "", 0, r.rating + m.total_rating, m.no_rating + 1, 
													min(r.timestamp, m.first_rated_ts), max(r.timestamp, m.last_rated_ts)),
			(m: MovieRating,n:MovieRating) => new MovieRating(m.movieId, "",
													(m.total_rating + n.total_rating)/ (m.no_rating + n.no_rating) , 
													m.total_rating + n.total_rating, m.no_rating + n.no_rating, 
													min(m.first_rated_ts, n.first_rated_ts), 
													max(m.last_rated_ts, n.last_rated_ts)))
		val movieJoinRatingRdd = movieRdd leftOuterJoin (combinedMovieRating)
		val mappedMovieJoinRatingRdd = movieJoinRatingRdd map (tuple => tuple._2 match {
				case (m, None) => new MovieRating(m.id, m.title, 0.0F, 0.0F, 0, 0L, 0L)
				case (m, Some(c)) => new MovieRating(m.id, m.title,  c.avg_rating, c.total_rating, 
												c.no_rating, c.first_rated_ts, c.last_rated_ts)
			})
		
		mappedMovieJoinRatingRdd
	}

	def returnRemovedHeader(rdd: RDD[String], firstHeaderField: String) : RDD[String] = {
		rdd.filter((line: String) => !(line startsWith firstHeaderField))
	}

	def getSparkContext() : SparkContext = {
		val conf = new SparkConf().setAppName("MovielensApp")

		new SparkContext(conf)
	}
}