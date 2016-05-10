package com.okmich.spark.mllatest.model

case class MovieRating(movieId: Int, title: String, avg_rating: Float, total_rating: Float, no_rating: Int, first_rated_ts: Long, last_rated_ts: Long)