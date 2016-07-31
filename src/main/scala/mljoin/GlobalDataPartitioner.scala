package mljoin


import org.apache.spark.Partitioner

class GlobalDataPartitioner (numParts:Int, B_i_data_hash: Data2 => Long) extends Partitioner {
  def getPartition(key: Any): Int = {
    key match {
      case null => 0
      case _ => {
        val tmp:Data2 = key.asInstanceOf[Data2]
        nonNegativeMod(B_i_data_hash(tmp), numParts)
      }
    }
  }
  
  def numPartitions: Int = numParts
  
  def nonNegativeMod(x: Long, mod: Int): Int = {
    val rawMod = (x % mod).toInt
    rawMod + (if (rawMod < 0) mod else 0)
  }
}