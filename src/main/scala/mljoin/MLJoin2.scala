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
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame

// Suffix "2" denotes version 2 and is just to avoid name clashes with our previous version

trait Model2 extends Serializable
trait Data2 extends Serializable
trait Delta2 extends Serializable
trait Output2 extends Serializable

trait Converters extends Serializable {
  // Override all the below methods and set the schemas
  def convertModelToRow(m:Model2):Row
  def convertRowToModel(r:Row): Model2
  def convertDataToRow(d:Data2): Row
  def convertRowToData(r:Row): Data2
  val modelSchema:StructType
  val dataSchema:StructType
}

class MLJoin2 extends Logging with Serializable {
    
    def joinNCoGroup(sqlContext:SQLContext, models :RDD[Model2], data :RDD[Data2], 
        converter:Converters, 
        g: Model2 => Data2 => Iterable[Delta2],
        // Here one first does efficient join (for example: hash-based join) and then follow it by filter
        columnToJoinOn:String,
        B_i: (Model2, Data2) => Boolean, 
        agg: (Iterable[Delta2], Data2) => Output2, method:String): RDD[Output2] = {
      
      import sqlContext.implicits._
      val modelsDF:DataFrame = sqlContext.createDataFrame(models.map(converter.convertModelToRow(_)), converter.modelSchema)
      val dataDF:DataFrame = sqlContext.createDataFrame(data.map(converter.convertDataToRow(_)), converter.modelSchema)
      modelsDF.registerTempTable("model")
      dataDF.registerTempTable("data")
      sqlContext.udf.register("B_i", B_i)
      
      // TODO: .....
      
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
                        val deltas: Iterable[Delta2] = models1.flatMap( model => 
                          // Cartesian join followed by selection predicate
                          if(B_i(model, data1)) g(model)(data1) else new ArrayList[Delta2]())
                        // Step 4b: Final output assembly
                        agg(deltas, data1)
                      } ) 
      }
      else if(method.compareToIgnoreCase("local") == 0) {
        // Step 2: Preparing parameters
        val param :RDD[(Long, (Model2, Data2 => Iterable[Delta2]))] = models
            .map(model => (model, g(model)))
            .flatMap(model => (0L to (count-1)).map(id => (id, model)))
        
        // Step 3: Cogroup
        val groupedParam :RDD[(Long, Iterable[(Model2, Data2 => Iterable[Delta2])])] = param.groupByKey()
        
        // Step 4: UDF invocation and final output assembly
        val foo = X.join(groupedParam)
                    .map( 
                      x => {
                        // For readability
                        val id1: Long = x._1
                        val data1: Data2 = x._2._1
                        val g1: Iterable[(Model2, Data2 => Iterable[Delta2])] = x._2._2
                        // Step 4a: UDF invocation
                        val deltas: Iterable[Delta2] = g1.flatMap( 
                            partiallyAppliedG => if(B_i(partiallyAppliedG._1, data1)) partiallyAppliedG._2(data1) else new ArrayList[Delta2]() )
                        // Step 4b: Final output assembly
                        agg(deltas, data1)
                      } )
      }
      else {
        throw new RuntimeException("Unsupported method:" + method)
      }
      ret
      
    }
    
    
    def joinNCoGroup(sqlContext:SQLContext, models :RDD[Model2], data :RDD[Data2], 
        g: Model2 => Data2 => Iterable[Delta2],
        B_i: (Model2, Data2) => Boolean, // Only option is to do cartesian join and follow it by filter
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
                        val deltas: Iterable[Delta2] = models1.flatMap( model => 
                          // Cartesian join followed by selection predicate
                          if(B_i(model, data1)) g(model)(data1) else new ArrayList[Delta2]())
                        // Step 4b: Final output assembly
                        agg(deltas, data1)
                      } ) 
      }
      else if(method.compareToIgnoreCase("local") == 0) {
        // Step 2: Preparing parameters
        val param :RDD[(Long, (Model2, Data2 => Iterable[Delta2]))] = models
            .map(model => (model, g(model)))
            .flatMap(model => (0L to (count-1)).map(id => (id, model)))
        
        // Step 3: Cogroup
        val groupedParam :RDD[(Long, Iterable[(Model2, Data2 => Iterable[Delta2])])] = param.groupByKey()
        
        // Step 4: UDF invocation and final output assembly
        val foo = X.join(groupedParam)
                    .map( 
                      x => {
                        // For readability
                        val id1: Long = x._1
                        val data1: Data2 = x._2._1
                        val g1: Iterable[(Model2, Data2 => Iterable[Delta2])] = x._2._2
                        // Step 4a: UDF invocation
                        val deltas: Iterable[Delta2] = g1.flatMap( 
                            partiallyAppliedG => if(B_i(partiallyAppliedG._1, data1)) partiallyAppliedG._2(data1) else new ArrayList[Delta2]() )
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