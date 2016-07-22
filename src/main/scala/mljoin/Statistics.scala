package mljoin

import java.util.concurrent.atomic.AtomicLong
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean

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
}