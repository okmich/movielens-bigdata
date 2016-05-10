package com.okmich.spark.mllatest.model

abstract class TimeValued(timestamp: Long) extends java.io.Serializable {
	import java.util.Calendar

	private val calendar = Calendar.getInstance
	calendar.setTimeInMillis(this.timestamp)

	val hour = calendar.get(Calendar.HOUR_OF_DAY)

	val minute = calendar.get(Calendar.MINUTE)

	val dayOfWeek = {
		import java.text.SimpleDateFormat

		new SimpleDateFormat("EEE").format(calendar.getTime())
	}

	val dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH)

	val month = calendar.get(Calendar.MONTH)

	val year = calendar.get(Calendar.YEAR)

	val amPm = if (calendar.get(Calendar.AM_PM) == Calendar.AM) 
					"AM"
				else
					"PM" 

	def SEP = "++" 

	override def toString = this.year +
			SEP + this.month +
			SEP + this.dayOfMonth +
			SEP + this.hour +
			SEP + this.minute +
			SEP + this.amPm  +
			SEP + this.dayOfWeek +
			SEP + this.timestamp
}
