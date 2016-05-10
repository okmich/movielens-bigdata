import com.okmich.spark.mllatest.model._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext


import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object MovieTagsApp {

	def main(args : Array[String]) : Unit = {
		if (args.length != 3){
			println("No arguments!!! Use <inputPath> <outputFolderPath> <format>")
			return; 
		}
		val input_path = args(0)
		val output_path = args(1)

		val sc = getSparkContext()

		val movieSourceRdd = sc.textFile(input_path + "/movies")
		val tagsSourceRdd = sc.textFile(input_path + "/tags")
		//remove headers
		val movieFilteredRdd = returnRemovedHeader(movieSourceRdd, "movieId")
		val tagsFilteredRdd = returnRemovedHeader(tagsSourceRdd, "userId")
		//map each line to objects
		val moviesRDD = movieFilteredRdd map (MovieCreator.lineToMovie(_))
		val tagsRDD = tagsFilteredRdd map (TagCreator.lineToTag(_))
		//remove stop words from tags
		
		//create a pair rdd for movie
		val moviePairRdd = moviesRDD map (movie => (movie.id, movie))
		
		//persists each cache
		moviePairRdd.persist(StorageLevel.DISK_ONLY)

		val movieTagsPairRdd = tagsRDD map (tag => (tag.movieId, tag))
		val movieTagsRdd = movieTagsPairRdd.groupByKey()
		movieTagsRdd.persist(StorageLevel.DISK_ONLY)

		//create the movie tag dataset
		val movieVsTagsRdd = getMovieTags(moviePairRdd, movieTagsRdd)

		//create the tag movies dataset
		val tagVsMoviesRdd = getTagMovies(movieTagsRdd)

		val format = args(2)
		if (format.toLowerCase == "text"){
			//output rdd to disk
			tagsRDD.saveAsTextFile(output_path + "/tags")
			movieTagsRdd.saveAsTextFile(output_path + "/movietags")
			movieVsTagsRdd.saveAsTextFile(output_path + "/movies_tags")
			tagVsMoviesRdd.saveAsTextFile(output_path + "/tags_movies")
		} else if (format.toLowerCase == "parquet"){
			val sqlCtx = new SQLContext(sc)
			import sqlCtx.implicits._ //unleashing the encoders and types

			//output rdd to disk as parquet
			tagsRDD.toDF().write.parquet(output_path + "/tags")
			movieTagsRdd.toDF().write.parquet(output_path + "/movietags")
			movieVsTagsRdd.toDF().write.parquet(output_path + "/parquet/movie_ratings")
			tagVsMoviesRdd.toDF().write.parquet(output_path + "/parquet/rating")
		} else {
			println("Invalid write format")
		}
	}

	def getMovieTags(movieRdd: RDD[(Int, Movie)], movieTagsRdd: RDD[(Int, Iterable[Tag])]): RDD[MovieTags] = {
		val movieJoinGrpKeyRdd = movieRdd leftOuterJoin movieTagsRdd

		movieJoinGrpKeyRdd map (tuple => tuple._2 match {
				case (m: Movie, None) => new MovieTags(tuple._1, m.title, List())
				case (m: Movie, Some(a)) =>  	
										new MovieTags(tuple._1, m.title, a.flatMap(_.tag.split(" ")).filter(_.length > 1).toList)
			})
	}

	def getTagMovies(movieTagsRdd: RDD[(Int, Iterable[Tag])]): RDD[TagMovies] = {
		//create the rdd of tag against list of movie ids
		val tagMovidIdPair : RDD[(String, Int)] = movieTagsRdd flatMap (mtr => for (t <- mtr._2) yield (t.tag.toLowerCase, mtr._1))	
		tagMovidIdPair.groupByKey() map (pair => TagMovies(pair._1, pair._2.toList))
	}

	def returnRemovedHeader(rdd: RDD[String], firstHeaderField: String) : RDD[String] = {
		rdd.filter((line: String) => !(line startsWith firstHeaderField))
	}


	def getSparkContext() : SparkContext = {
		val conf = new SparkConf().setAppName("MovielensApp")

		new SparkContext(conf)
	}
}