package com.okmich.spark.mllatest.model

case class Movie(id: Int, title: String, year:Int, isAction : Byte = 0, isAdventure : Byte = 0, isAnimation : Byte = 0, 
	isChildren : Byte = 0, isComedy: Byte = 0, isCrime: Byte = 0, isDocumentary: Byte = 0, isDrame: Byte = 0, 
	isFantasy: Byte = 0, isFilmNoir: Byte = 0, isHorror : Byte = 0, isIMAX: Byte = 0, isMusical: Byte = 0, isMystery: Byte = 0,
	isRomance: Byte = 0, isScifi: Byte = 0, isThriller: Byte = 0, isWar: Byte = 0, isWestern: Byte = 0){

	private val SEP = "++"


	override def toString = this.id + SEP + this.title + SEP + this.year + 
					SEP + this.isAction + 
					SEP + this.isAdventure +
					SEP + isAnimation + 
					SEP + isChildren + 
					SEP + isComedy + 
					SEP + isChildren + 
					SEP + isCrime + 
					SEP + isDocumentary + 
					SEP + isDrame + 
					SEP + isFantasy + 
					SEP + isFilmNoir + 
					SEP + isHorror + 
					SEP + isIMAX + 
					SEP + isMusical + 
					SEP + isMystery + 
					SEP + isRomance + 
					SEP + isScifi + 
					SEP + isThriller + 
					SEP + isWar + 
					SEP + isWestern
}

case class MovieTags(movieId: Int, name: String, tags:List[String]){

	val SEP = "++"

	override def toString = movieId +
			SEP + tags.mkString("::")
}

object MovieCreator {
	
	def isGenre(args : Array[String], genre : String) : Byte = {
		if (args contains genre) 1
		else 0
	}


	def lineToMovie(line : String) : Movie = {
		val fields = line.split(",")
		val tempMsg = new java.lang.StringBuffer

		val title = if (fields.length == 3) fields(1)
					else if (fields.length > 3) {
						for (i <- 1 to fields.length - 2) tempMsg.append(" ").append(fields(i)); tempMsg.toString.replace("\"","").trim()
					}
					else tempMsg.toString

		val year = if (title.substring(title.length - 6, title.length).matches("\\(\\d{4}\\)"))
						title.substring(title.length - 5, title.length - 1)
				   else "0000"	
				   
		val genres = fields(fields.size - 1) split "\\|"
		new Movie(fields(0).toInt, title, year.toInt,
			isGenre(genres, "Adventure"),
			isGenre(genres, "Animation"),
			isGenre(genres, "Children"),
			isGenre(genres, "Comedy"),
			isGenre(genres, "Crime"),
			isGenre(genres, "Documentary"),
			isGenre(genres, "Drama"),
			isGenre(genres, "Fantasy"),
			isGenre(genres, "Film-Noir"),
			isGenre(genres, "Horror"),
			isGenre(genres, "IMAX"),
			isGenre(genres, "Musical"),
			isGenre(genres, "Mystery"),
			isGenre(genres, "Romance"),
			isGenre(genres, "Sci-Fi"),
			isGenre(genres, "Thriller"),
			isGenre(genres, "War"),
			isGenre(genres, "Western")
		)
	}

}