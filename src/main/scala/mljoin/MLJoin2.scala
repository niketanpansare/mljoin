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

// Suffix "2" denotes version 2 and is just to avoid name clashes with our previous version
trait Model2 extends Serializable
trait Data2 extends Serializable
trait Delta2 extends Serializable
trait Output2 extends Serializable

class CustomRow(id:Long, hash:Long, foo:Array[Byte]) extends Row with Serializable {
  override def copy() = (new CustomRow(id, hash, foo)).asInstanceOf[Row]
  override def get(i: Int) = if(i == 0) id else if(i == 1) hash else foo
  override def length = 3
  override def apply(i: Int): Any = if(i == 0) id else if(i == 1) hash else foo
  override def getBoolean(i: Int): Boolean = ???
  override def getByte(i: Int): Byte = ???
  override def getDouble(i: Int): Double = ???
  override def getFloat(i: Int): Float = ???
  override def getInt(i: Int): Int = ???
  override def getLong(i: Int): Long = ???
  override def getShort(i: Int): Short = ???
  override def getString(i: Int): String = ???
  override def isNullAt(i: Int): Boolean = ???
  override def toSeq: Seq[Any] = ???
}

case class TestModel2(val1: Int, val2: Int) extends Model2
case class TestData2(val1: Int, val2: Int) extends Data2
case class TestDelta2(val1: Int, val2: Int) extends Delta2
case class TestOutput2(val1: Int, val2: Int) extends Output2

// val res = (new mljoin.Test).test1(sc, sqlContext, "local")
// res.take(3)
// should output: Array[mljoin.Output2] = Array(TestOutput2(-1987944601,2017986180), TestOutput2(-2026790272,1979140509), TestOutput2(-1848329172,-2137365687))
class Test extends Logging with Serializable {
    def test1(sc:SparkContext, sqlContext:SQLContext, method:String) = {
      val models = sc.parallelize(1 to 10).map(x => { val r = new java.util.Random(); new TestModel2(r.nextInt, r.nextInt).asInstanceOf[Model2]})
      val data = sc.parallelize(1 to 10).map(x => { val r = new java.util.Random(); new TestData2(r.nextInt, r.nextInt).asInstanceOf[Data2]})
      def test_B_i_data_hash(d:Data2) = 1
      def test_B_i_model_hash(m:Model2) = 1
      def test_B_i(m:Model2, d:Data2):Boolean = {
        val m1: TestModel2 = m.asInstanceOf[TestModel2]  
        val d1: TestData2 = d.asInstanceOf[TestData2]
        // Some random criteria
        if(m1.val1 > d1.val1) true else false
      }
      def test_g1(m:Model2) = {
        System.out.println(">" + m.asInstanceOf[TestModel2].val1)
        new TestDelta2(m.asInstanceOf[TestModel2].val1, m.asInstanceOf[TestModel2].val2)
      }
      def test_g(m:Model2)(d:Data2):Iterable[Delta2] = {
        val ret = new ArrayList[Delta2]
        val del = test_g1(m)
        System.out.println("-" + del.val1)
        ret.add(new TestDelta2(del.val1 + d.asInstanceOf[TestData2].val1,
            del.val2 + d.asInstanceOf[TestData2].val2).asInstanceOf[TestDelta2])
        ret
      }
//      def old_test_g(m:Model2)(d:Data2):Iterable[Delta2] = {
//        val ret = new ArrayList[Delta2]
//        ret.add(new TestDelta2(m.asInstanceOf[TestModel2].val1 + d.asInstanceOf[TestData2].val1,
//            m.asInstanceOf[TestModel2].val2 + d.asInstanceOf[TestData2].val2).asInstanceOf[TestDelta2])
//        ret
//      }
      def test_agg(it:Iterable[Delta2], d:Data2):Output2 = {
        new TestOutput2(d.asInstanceOf[TestData2].val1-it.head.asInstanceOf[TestDelta2].val1, 
           d.asInstanceOf[TestData2].val2-it.head.asInstanceOf[TestDelta2].val2)
      }
      (new MLJoin2(test_g _, test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _)).joinNCoGroup(sqlContext, models, data, "naive", false)
    }
}

class MLJoin2(g: Model2 => Data2 => Iterable[Delta2],
        // For natural join: 
        B_i_model_hash: Model2 => Long,
        B_i_data_hash: Data2 => Long,
        B_i: (Model2, Data2) => Boolean,
        agg: (Iterable[Delta2], Data2) => Output2) extends Logging with Serializable {
 
  def convertDataToDF(sqlContext:SQLContext, X:RDD[(Long, Data2)], 
        B_i_data_hash: Data2 => Long): DataFrame = {
      // Instead of UDT which will get removed in Spark 2.0 https://issues.apache.org/jira/browse/SPARK-14155
      val dataType:StructType = new StructType(
          Array(new StructField("id", LongType, nullable=false), 
              new StructField("hash", LongType, nullable=false), 
              new StructField("data", BinaryType, nullable=false)))
      sqlContext.createDataFrame(X.map( x => (new CustomRow(x._1,  B_i_data_hash(x._2), serialize(x._2))).asInstanceOf[Row] ), dataType)
    }
    
    def convertModelToDF(sqlContext:SQLContext, param :RDD[(Long, Model2)], 
        B_i_model_hash: Model2 => Long): DataFrame = {
      // Instead of UDT which will get removed in Spark 2.0 https://issues.apache.org/jira/browse/SPARK-14155
      val modelType:StructType = new StructType(
          Array(new StructField("id", LongType, nullable=false), 
            new StructField("hash", LongType, nullable=false), 
            new StructField("model", BinaryType, nullable=false)))
      sqlContext.createDataFrame(param.map( x => (new CustomRow(x._1,  B_i_model_hash(x._2), serialize(x._2))).asInstanceOf[Row] ), modelType)
    }
    
    def convertLocalModelToDF(sqlContext:SQLContext, param :RDD[(Long, (Model2, Data2 => Iterable[Delta2]))], 
        B_i_model_hash: Model2 => Long): DataFrame = {
      // Instead of UDT which will get removed in Spark 2.0 https://issues.apache.org/jira/browse/SPARK-14155
      val modelType:StructType = new StructType(
          Array(new StructField("id", LongType, nullable=false), 
            new StructField("hash", LongType, nullable=false), 
            new StructField("model", BinaryType, nullable=false)))
      sqlContext.createDataFrame(param.map( x => (new CustomRow(x._1,  B_i_model_hash(x._2._1), serialize(x._2))).asInstanceOf[Row] ), modelType)
    }
    
    def serialize(o:Object):Array[Byte] = SerializeUtils.serialize(o)
    def deserialize(b:Array[Byte]):Object = SerializeUtils.deserialize(b)
        
    def serialized_B_i(m: Array[Byte], d:Array[Byte]): Boolean = B_i(deserialize(m).asInstanceOf[Model2], deserialize(d).asInstanceOf[Data2])
    
    def serialized_local_B_i(m: Array[Byte], d:Array[Byte]): Boolean = B_i(deserialize(m).asInstanceOf[(Model2, Data2 => Iterable[Delta2])]._1, deserialize(d).asInstanceOf[Data2])
    
    def joinNCoGroup(sqlContext:SQLContext, models :RDD[Model2], data :RDD[Data2], method:String, applyHash:Boolean): RDD[Output2] = {
      
      // Step 1: Seeding
      val X :RDD[(Long, Data2)] = data.zipWithIndex().map(x => (x._2, x._1.asInstanceOf[Data2])).persist(StorageLevel.MEMORY_AND_DISK)
      val count = X.count
      val XDF:DataFrame = convertDataToDF(sqlContext, X, B_i_data_hash)
      
      val sqlStr = if(applyHash)
                      """
                      SELECT d.id, m.model, d.data
                      FROM Model m, Data d
                      WHERE m.hash = d.hash
                      AND B_i(m.model, d.data)
                      """
                  else
                      """
                      SELECT d.id, m.model, d.data
                      FROM Model m, Data d
                      WHERE B_i(m.model, d.data)
                      """
      
      var ret :RDD[Output2] = null
      if(method.compareToIgnoreCase("naive") == 0) {
        // Step 2: Preparing parameters
        val param :RDD[(Long, Model2)] = models.zipWithIndex().map(x => (x._2, x._1))
        
        val paramDF:DataFrame = convertModelToDF(sqlContext, param, B_i_model_hash)
        
        XDF.registerTempTable("Data")
        paramDF.registerTempTable("Model")
        
        sqlContext.udf.register("B_i", serialized_B_i _)
        
        // Join
        val temp:RDD[(Long, Model2, Data2)] = sqlContext.sql(sqlStr).rdd
                      .map(r => (r.get(0).asInstanceOf[Long],
                           deserialize(r.get(1).asInstanceOf[Array[Byte]]).asInstanceOf[Model2],
                           deserialize(r.get(2).asInstanceOf[Array[Byte]]).asInstanceOf[Data2]))
          
        // Step 3: Cogroup
        // Step 4: UDF invocation and final output assembly
        // TODO: Jacob: Please double check final output assembly
        ret = temp.map(x => ((x._1, x._3), x._2))
                         .groupByKey()
                         .flatMap(x => {
                           val d:Data2 = x._1._2
                           val id:Long = x._1._1
                           val models:Iterable[Model2] = x._2
                           models.map(g(_)(d)).map(agg(_, d)).map((id, _))
                           }  
                         ).values //.sortByKey().values
        
      }
      else if(method.compareToIgnoreCase("local") == 0) {
        // Step 2: Preparing parameters
        val param :RDD[(Long, (Model2, Data2 => Iterable[Delta2]))] = models
            .zipWithIndex()
            .map(x => (x._2, (x._1, g(x._1))))
            
        val paramDF:DataFrame = convertLocalModelToDF(sqlContext, param, B_i_model_hash)
        
        XDF.registerTempTable("Data")
        paramDF.registerTempTable("Model")
        
        sqlContext.udf.register("B_i", serialized_local_B_i _)
        
        // Join
        val temp:RDD[(Long, (Model2, Data2 => Iterable[Delta2]), Data2)] = sqlContext.sql(sqlStr).rdd
                      .map(r => (r.get(0).asInstanceOf[Long],
                           deserialize(r.get(1).asInstanceOf[Array[Byte]]).asInstanceOf[(Model2, Data2 => Iterable[Delta2])],
                           deserialize(r.get(2).asInstanceOf[Array[Byte]]).asInstanceOf[Data2]))
        
        // Step 3: Cogroup
        // Step 4: UDF invocation and final output assembly
        // TODO: Jacob: Please double check final output assembly
        ret = temp.map(x => ((x._1, x._3), x._2))
                         .groupByKey()
                         .flatMap(x => {
                           val d:Data2 = x._1._2
                           val id:Long = x._1._1
                           val models:Iterable[(Model2, Data2 => Iterable[Delta2])] = x._2
                           models.map(_._2(d)).map(agg(_, d)).map((id, _))
                         }).values //.sortByKey().values
      }
      else {
        throw new RuntimeException("Unsupported method:" + method)
      }
      ret
      
    }
}