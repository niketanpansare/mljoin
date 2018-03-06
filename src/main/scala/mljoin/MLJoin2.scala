package mljoin

import breeze.linalg._
import breeze.numerics._
import org.apache.spark.broadcast._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import java.util.ArrayList
import java.util.HashSet
import java.util.Random
import org.apache.spark.{ Logging, SparkConf }
//import akka.actor.{ Actor, ActorRef, ActorSystem, ActorSelection }
//import akka.actor.Props
import org.apache.spark.SparkEnv
import org.apache.spark.storage.StorageLevel
//import akka.actor.{ Props, Deploy, Address, AddressFromURIString }
//import akka.remote.RemoteScope
import java.util.concurrent.ConcurrentHashMap
import java.io._
import java.net._
import scala.collection.JavaConversions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.HashPartitioner

// Suffix "2" denotes version 2 and is just to avoid name clashes with our previous version
trait Model2 extends Serializable
trait Data2 extends Serializable
trait Delta2 extends Serializable
trait Output2 extends Serializable

trait DataPart1 extends Serializable
trait DataPart2 extends Serializable

class MLJoin2(
        // For natural join: 
        B_i_model_hash: Model2 => Long,
        B_i_data_hash: Data2 => Long,
        B_i: (Model2, Data2) => Boolean,
        agg: (Iterable[Delta2], Data2) => Output2,
        combiner: (Iterable[Delta2], Data2) => Iterable[Delta2], 
        broadcastBasedJoin:Boolean = true) extends Logging with Serializable {
    
    // Step 1: Seeding here refers to the process of appending each data item from the set X with a unique identifier.
    def seeding(data :RDD[Data2], applyHash:Boolean):RDD[(Long, Long, Data2)] = {
      val start = System.nanoTime()
      val data1:RDD[(Long, Long, Data2)] = data.zipWithIndex().map(x => 
         if(applyHash) (x._2, B_i_data_hash(x._1), x._1)
         else (x._2, 0L, x._1))
      seedingTime = (System.nanoTime() - start)*(1e-9)
      data1
    }
    
    // Step 2 and3: Prepare the parameter (i.e. perform map-side join) and cogroup
    // Step 2.a: broadcast the seeded model 
    def broadcastLocalModelNew(sqlContext:SQLContext, models :RDD[Model2], applyHash:Boolean, g1: Model2 => Object): Broadcast[Array[(Long, Object)]] = {
      val start = System.nanoTime()
      val modelWithHash:Array[(Long, Object)] = models.map(x => if(applyHash) (B_i_model_hash(x), g1(x)) else (0L, g1(x))).collect
      val model1 = sqlContext.sparkContext.broadcast(modelWithHash)
      globalModelBroadcastTime = (System.nanoTime() - start)*(1e-9)
      model1
    }
    // Step 2.b: join seeded model and seeded data and cogroup to return RDD[(Long i.e. hash, Data2 i.e. X, Object)]
    def mapSideJoinLocal(data1:RDD[(Long, Long, Data2)], model1:Broadcast[Array[(Long, Object)]]):RDD[(Long, Data2, Object)] = {
      val start = System.nanoTime()
      val ret: RDD[(Long, Data2, Object)] = data1.flatMap(x => {
            val m:Array[(Long, Object)] = model1.value
            val id:Long = x._1
            val data_hash:Long = x._2
            val d:Data2 = x._3
            m.filter(_._1 == data_hash).map(m1 => (id, d, m1._2))
      })
      joinTime = (System.nanoTime() - start)*(1e-9)
      ret
    }
    // Step 4: UDF invocation: For each data item x, the user-defined function g() is evaluated with x. 
    // g() can then produce zero or more outputs per input cogroup (hence returns an iterable).
    def applyUDFLocal(joinedRDD:RDD[(Long, Data2, Object)], 
        g2: (Object, Data2) => Iterable[Delta2]): RDD[((Long, Data2), Iterable[Delta2])] = {
      val start = System.nanoTime()
      val ret = joinedRDD.map(x => {
            val id:Long = x._1
            val d:Data2 = x._2
            val m:Object = x._3
            ( (id, d), g2(m, d) )
          })
      applyUDFTime = (System.nanoTime() - start)*(1e-9)
      ret
    }
    
    def performAggregation(in:RDD[((Long, Data2), Iterable[Delta2])]): RDD[Output2] = {
      val start = System.nanoTime()
//      Older version without combiner:
//      val ret = in.reduceByKey((it1, it2) => it1 ++ it2)
//                    .map(x => {
//                      val id:Long = x._1._1
//                      val d:Data2 = x._1._2
//                      val itDelta:Iterable[Delta2] = x._2 
//                      (id, agg(itDelta, d))
//                    }).values.persist(StorageLevel.MEMORY_AND_DISK)
      val ret = in.map(e => (e._1._1, (e._1._2, e._2)))
                  .reduceByKey((it1, it2) => {
                     val d:Data2 = it1._1 // should be same as it2._1 
                     val deltas:Iterable[Delta2] = it1._2 ++ it2._2
                     (d, combiner(deltas, d))
                    })
                   .map(e => {
                     val d:Data2 = e._2._1
                     val deltas:Iterable[Delta2] = e._2._2
                     agg(deltas, d)
                   }).persist(StorageLevel.MEMORY_AND_DISK)
      ret.count //.sortByKey().values
      aggregationTime  = (System.nanoTime() - start)*(1e-9)              
      ret
    }
    
    def printStatsNew() = {
      System.out.println("Seeding: " + seedingTime + " sec.")
      System.out.println("Global model broadcast time: " + globalModelBroadcastTime + " sec.")
      System.out.println("Global model shuffle time: " + globalModelShuffleTime + " sec.")
      System.out.println("Global data shuffle time: " + globalDataShuffleTime + " sec.")
      System.out.println("Apply UDF time: " + applyUDFTime + " sec.")
      System.out.println("Join time: " + joinTime + " sec.")
      System.out.println("Output Assembly: " + aggregationTime + " sec.")
    }
    
    def joinNCoGroupLocalNew(sqlContext:SQLContext, models :RDD[Model2], data :RDD[Data2], method:String, 
        applyHash:Boolean,
        g1: Model2 => Object, g2: (Object, Data2) => Iterable[Delta2]): RDD[Output2] = {
      
      val totalStart = System.nanoTime()
      val out = 
        if(method.compareToIgnoreCase("local") == 0) {
          if(broadcastBasedJoin) {
            performAggregation(
              applyUDFLocal(
                mapSideJoinLocal(
                    seeding(data, applyHash), 
                    broadcastLocalModelNew(sqlContext, models, applyHash, g1)
                ), g2))
          }
          else {
            throw new RuntimeException("Unsupported")
//            // ---------------------------------------------------------------------------
//            // Specialized non-map side join helper functions
//            def nonMapSideJoinLocal(data1:RDD[(Long, Long, Data2)], model1:RDD[(Long, Object)]):RDD[(Long, Data2, Object)] = {
//              val start = System.nanoTime()
//              val ret = data1.map(x => (x._2, (x._1, x._3))).join(model1).map(y => (y._2._1._1, y._2._1._2, y._2._2))
//              joinTime = (System.nanoTime() - start)*(1e-9)
//              ret
//            }
//            
//             performAggregation(
//              applyUDFLocal(
//                nonMapSideJoinLocal(
//                    seeding(data, applyHash), 
//                    models.map(x => if(applyHash) (B_i_model_hash(x), g1(x)) else (0L, g1(x)))
//                ), g2))
//            // ---------------------------------------------------------------------------
          }
        }
        else if(method.compareToIgnoreCase("global") == 0) {
          // For now keeping this static as we don't know cardinality of hash function
          // Also we need to ensure that we donot reduce the parallelism
          numPartitions = sqlContext.sparkContext.defaultParallelism // Math.min( models.getNumPartitions, data.getNumPartitions ) 
          
          val t1 = System.nanoTime()
          val repartitionedModel: RDD[(Long, Object)] = models.map(x => 
                                   if(applyHash) (x, B_i_model_hash(x))
                                   else throw new RuntimeException("Expected applyHash"))
                                  .partitionBy(new GlobalModelPartitioner(numPartitions, B_i_model_hash))
                                  .map(_.swap)
                                  .map(x => (x._1, g1(x._2)))
                                  .persist(StorageLevel.MEMORY_AND_DISK)
          repartitionedModel.count
          globalModelShuffleTime = (System.nanoTime() - t1)*(1e-9)
          
          val dat1 = seeding(data, applyHash)
          
          val t2 = System.nanoTime()
          val repartitionedData: RDD[(Long, (Long, Data2))] = dat1.map(x => 
                                   (x._3, (x._2, x._1)))
                                  .partitionBy(new GlobalDataPartitioner(numPartitions, B_i_data_hash))
                                  .map(x => (x._2._1, (x._2._2, x._1)))
                                  .persist(StorageLevel.MEMORY_AND_DISK)
          repartitionedData.count
          globalDataShuffleTime = (System.nanoTime() - t2)*(1e-9)
          
          val t3 = System.nanoTime()
          val joinedRDD:RDD[(Long, Data2, Object)] = repartitionedData.join(repartitionedModel)
                                                      .map(y =>  {
                                                        val id:Long = y._2._1._1
                                                        val d:Data2 = y._2._1._2
                                                        val m:Object = y._2._2
                                                        (id, d, m)
                                                      })//.persist(StorageLevel.MEMORY_AND_DISK)
//          joinedRDD.count
          joinTime = (System.nanoTime() - t3)*(1e-9)
          
          performAggregation(
            applyUDFLocal(joinedRDD, g2))
          
        }
        else {
          throw new RuntimeException("The method is not supported:" + method)
        }
      System.out.println("Total Time: " + (System.nanoTime() - totalStart)*(1e-9) + " sec.")
      printStatsNew()
      out
    }
    
    var seedingTime:Double = 0
    var joinTime: Double = 0
    var applyUDFTime: Double = 0
    var aggregationTime: Double = 0
    var globalModelBroadcastTime:Double = 0
    
    var globalModelShuffleTime:Double = 0
    var globalDataShuffleTime:Double = 0
    
    // Jacob invoke this method instead
    def joinNCoGroupLDALocalTwoData(sqlContext:SQLContext, models :RDD[Model2],
        // ---------------------------------
        // Instead of RDD[Data2]
        data1:RDD[(Int, LDADataPart1)], data2:RDD[(Int, LDADataPart2)], mergeFn: (LDADataPart1, LDADataPart2) => Data2, 
        // ---------------------------------
        method:String, 
        applyHash:Boolean,
        g1: Model2 => Object, g2: (Object, Data2) => Iterable[Delta2]): RDD[Output2] = {
     joinNCoGroupLocalNew(sqlContext, models, 
         data1.join(data2).map(x => mergeFn(x._2._1, x._2._2)), 
         method, applyHash, g1, g2)
    }
    
    var numPartitions = -1
    
    
}