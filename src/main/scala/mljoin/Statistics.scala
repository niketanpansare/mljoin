package mljoin

import java.util.concurrent.atomic.AtomicLong
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.spark.rdd._

object Statistics {
  val serializeTime = new AtomicLong
  val deserializeTime = new AtomicLong
  val serialized_B_i = new AtomicLong
  val serialized_simulated_local_B_i = new AtomicLong
  val serialized_local_B_i = new AtomicLong
  val seeding = new AtomicLong
  val prepareParameters = new AtomicLong
  val doSparkSQLJoinNCoGroup = new AtomicLong
  val groupByKeyFlatMapApplication = new AtomicLong
  
  
  // Assumption no parallel joinNCoGroup
  @volatile var isFetched = false
  
  def get(): ArrayList[Long] = {
    val ret:ArrayList[Long] = new ArrayList[Long]
    Statistics.synchronized {
      if(!isFetched) {
        isFetched = true
        ret.add(serializeTime.get)
        ret.add(deserializeTime.get)
        ret.add(serialized_B_i.get)
        ret.add(serialized_simulated_local_B_i.get)
        ret.add(serialized_local_B_i.get)
        ret.add(seeding.get)
        ret.add(prepareParameters.get)
        ret.add(doSparkSQLJoinNCoGroup.get)
        ret.add(groupByKeyFlatMapApplication.get)
        serializeTime.set(0)
        deserializeTime.set(0)
        serialized_B_i.set(0)
        serialized_simulated_local_B_i.set(0)
        serialized_local_B_i.set(0)
        seeding.set(0)
        prepareParameters.set(0)
        doSparkSQLJoinNCoGroup.set(0)
        groupByKeyFlatMapApplication.set(0)
      }
    }
    ret
  }
  
  def printStatistics(X:RDD[Data2]): Unit = {
      val stats = X.map(x => Statistics.get()).filter(_.size() > 0).reduce((x, y) => {
        val ret11 = new ArrayList[Long]
        for(i <- 0 until y.size()) {
          ret11.add(y.get(i) + x.get(i))
        }
        ret11
      })
      // ---------------------------------------------------------------------------------------------------
      System.out.println("The statistics for this run are:")
      System.out.print("Serialization time: " +  stats.get(0)*(1e-9) + " sec.\n")
      System.out.print("Deserialization time: " + stats.get(1)*(1e-9) + " sec.\n")
      System.out.print("serialized_B_i time: " + stats.get(2)*(1e-9) + " sec.\n")
      System.out.print("serialized_simulated_local_B_i time: " +   stats.get(3)*(1e-9) + " sec.\n")
      System.out.print("serialized_local_B_i time: " +   stats.get(4)*(1e-9) + " sec.\n")
      System.out.print("seeding time: " +   stats.get(5)*(1e-9) + " sec.\n")
      System.out.print("prepareParameters time: " +   stats.get(6)*(1e-9) + " sec.\n")
      System.out.print("doSparkSQLJoinNCoGroup time: " +   stats.get(7)*(1e-9) + " sec.\n")
      System.out.print("groupByKeyFlatMapApplication time: " +   stats.get(8)*(1e-9) + " sec.\n")
      // ---------------------------------------------------------------------------------------------------
    }
}