package com.okmich.spark.mllatest.model

case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long) extends TimeValued(timestamp: Long) {

	override def toString = this.userId + SEP + this.movieId +
			SEP + this.rating + super.toString
	
}

object RatingCreator {
	
	def lineToRating(line : String) : Rating = {
		val fields = line.split(",")

		new Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
	}
}