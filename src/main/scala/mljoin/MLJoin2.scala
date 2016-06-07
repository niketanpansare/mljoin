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

// Suffix "2" denotes version 2 and is just to avoid name clashes with our previous version

trait Model2 extends Serializable
trait Data2 extends Serializable
trait Delta2 extends Serializable
trait Output2 extends Serializable

class MLJoin2 extends Logging with Serializable {
  
    def joinNCoGroup(sc :SparkContext, models :RDD[Model2], data :RDD[Data2], 
        g: Model2 => Data2 => Iterable[Delta2],
        agg: (Iterable[Delta2], Data2) => Output2, method:String): RDD[Output2] = {
      // Step 1: Seeding
      val X :RDD[(Long, Data2)] = data.zipWithIndex().map(x => (x._2, x._1)).persist(StorageLevel.MEMORY_AND_DISK)
      val count = X.count
      
      var ret :RDD[Output2] = null
      if(method.compareToIgnoreCase("naive") == 0) {
        // Step 2: Preparing parameters
        val param :RDD[(Long, Model2)] = models.flatMap( model => (0L to (count-1)).map(id => (id, model)) )
        
        // Step 3: Cogroup
        val groupedParam :RDD[(Long, Iterable[Model2])] = param.groupByKey()
        
        // Step 4: UDF invocation and final output assembly
        // TODO: Jacob: Please double check final output assembly
        ret = X.join(groupedParam) // Will be one-to-one join because of earlier groupByKey
                .map( 
                      x => {
                        // For readability
                        val id1: Long = x._1
                        val data1: Data2 = x._2._1
                        val models1: Iterable[Model2] = x._2._2
                        // Step 4a: UDF invocation
                        val deltas: Iterable[Delta2] = models1.flatMap( model => g(model)(data1) )
                        // Step 4b: Final output assembly
                        agg(deltas, data1)
                      } ) 
      }
      else if(method.compareToIgnoreCase("local") == 0) {
        // Step 2: Preparing parameters
        val param :RDD[(Long, Data2 => Iterable[Delta2])] = models.map(model => g(model)).flatMap( model => (0L to (count-1)).map(id => (id, model)) )
        
        // Step 3: Cogroup
        val groupedParam :RDD[(Long, Iterable[Data2 => Iterable[Delta2]])] = param.groupByKey()
        
        // Step 4: UDF invocation and final output assembly
        val foo = X.join(groupedParam)
                    .map( 
                      x => {
                        // For readability
                        val id1: Long = x._1
                        val data1: Data2 = x._2._1
                        val g1: Iterable[Data2 => Iterable[Delta2]] = x._2._2
                        // Step 4a: UDF invocation
                        val deltas: Iterable[Delta2] = g1.flatMap( partiallyAppliedG => partiallyAppliedG(data1) )
                        // Step 4b: Final output assembly
                        agg(deltas, data1)
                      } )
      }
      else {
        throw new RuntimeException("Unsupported method:" + method)
      }
      ret
      
    }
    
}