import org.apache.spark.Partitioner

class MovieKeyPartitioner(val numParts: Int, val range: Int) extends Partitioner{

	def getPartition(key: Any): Int = 0
	// {
	// 	val id = key.asInstanceOf[Int]

	// 	1
	// }

	def numPartitions = this.numParts
}