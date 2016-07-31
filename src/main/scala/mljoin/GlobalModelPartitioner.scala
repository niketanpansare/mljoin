package mljoin

import org.apache.spark.Partitioner

class GlobalModelPartitioner (numParts:Int, B_i_model_hash: Model2 => Long) extends Partitioner {
  def getPartition(key: Any): Int = {
    key match {
      case null => 0
      case _ => {
        val tmp:Model2 = key.asInstanceOf[Model2]
        nonNegativeMod(B_i_model_hash(tmp), numParts)    
      }
    }
  }
  
  def numPartitions: Int = numParts
  
  def nonNegativeMod(x: Long, mod: Int): Int = {
    val rawMod = (x % mod).toInt
    rawMod + (if (rawMod < 0) mod else 0)
  }
}