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

// val t = new mljoin.Test
// val data = t.generateSampleData(sc)
// val res = t.test1(sc, sqlContext, "local", data)
// res.take(3)
// mljoin.Statistics.printStatistics(data)
// should output: Array[mljoin.Output2] = Array(TestOutput2(-1987944601,2017986180), TestOutput2(-2026790272,1979140509), TestOutput2(-1848329172,-2137365687))
class Test extends Logging with Serializable {
  
    def generateSampleData(sc:SparkContext): RDD[Data2] = sc.parallelize(1 to 10).map(x => { val r = new java.util.Random(); new TestData2(r.nextInt, r.nextInt).asInstanceOf[Data2]}) 
    def test1(sc:SparkContext, sqlContext:SQLContext, method:String, data:RDD[Data2]) = {
      val models = sc.parallelize(1 to 10).map(x => { val r = new java.util.Random(); new TestModel2(r.nextInt, r.nextInt).asInstanceOf[Model2]})
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
      def test_local_g1(m:Model2): Object = {
        System.out.println(">" + m.asInstanceOf[TestModel2].val1)
        new TestDelta2(m.asInstanceOf[TestModel2].val1, m.asInstanceOf[TestModel2].val2)
      }
      
      def test_local_g2(del1:Object, d:Data2): Iterable[Delta2] = {
        val ret = new ArrayList[Delta2]
        val del = del1.asInstanceOf[TestDelta2]
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
      if(method.compareToIgnoreCase("naive") == 0 || method.compareToIgnoreCase("simulated-local") == 0) {
        (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _))
        .joinNCoGroup(sqlContext, models, data, method, false, test_g _)
      }
      else if(method.compareToIgnoreCase("local") == 0) {
        (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _))
        .joinNCoGroupLocal(sqlContext, models, data, method, false, test_local_g1 _, test_local_g2 _)
      }
      else {
        (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _))
        .joinNCoGroupLocal(sqlContext, models, data, method, true, test_local_g1 _, test_local_g2 _)
      }
    }
    
    def preprocessLDA(line: String): Data2 = {
      val splits = line.split('|')
      val text = splits(2).drop(1).dropRight(1).split(',')
      val wordsInDoc = Array.ofDim[Int](text.length / 2)
      val wordCounts = Array.ofDim[Int](text.length / 2)
      for (i <- 0 until text.length / 2) {
        wordsInDoc(i) = java.lang.Integer.parseInt(text(2 * i))
        wordCounts(i) = java.lang.Integer.parseInt(text(2 * i + 1))
      }
      new LDAData2(java.lang.Integer.parseInt(splits(0)), java.lang.Integer.parseInt(splits(1)), wordsInDoc, wordCounts).asInstanceOf[Data2]
    }
    
    // val data = sc.textFile(initialData).map(t.preprocessLDA)
    def testLDA(sc:SparkContext, sqlContext:SQLContext, method:String, data:RDD[Data2]) = {
      val start = System.nanoTime()
      val models = sc.parallelize(0 to (LDAData2.WB-1)).map(x => new LDAModel2(x).asInstanceOf[Model2])
      def test_B_i_data_hash(d:Data2) = 1
      def test_B_i_model_hash(m:Model2) = 1
      def test_B_i(m:Model2, d:Data2):Boolean = {
        val m1: LDAModel2 = m.asInstanceOf[LDAModel2]  
        val d1: LDAData2 = d.asInstanceOf[LDAData2]
        if(m1.wordBlockID == d1.wordBlockID) true else false
      }
      def test_g(m:Model2)(d:Data2):Iterable[Delta2] = {
        m.asInstanceOf[LDAModel2].process()
        d.asInstanceOf[LDAData2].process(m)
      }
      def test_local_g1(m:Model2): Object = {
        m.asInstanceOf[LDAModel2].process()
      }
      
      def test_local_g2(del1:Object, d:Data2): Iterable[Delta2] = {
        val m1 = del1.asInstanceOf[Model2]
        d.asInstanceOf[LDAData2].process(m1)
      }
      def test_agg(it:Iterable[Delta2], d:Data2):Output2 = {
        val ret = new LDAOutput2();
        for (d <- it) {
          ret.addTuple(d.asInstanceOf[LDADelta2])
        }
        ret
      }
      val ret = if(method.compareToIgnoreCase("naive") == 0 || method.compareToIgnoreCase("simulated-local") == 0) {
        (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _))
        .joinNCoGroup(sqlContext, models, data, method, false, test_g _)
      }
      else if(method.compareToIgnoreCase("local") == 0) {
        (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _))
        .joinNCoGroupLocal(sqlContext, models, data, method, false, test_local_g1 _, test_local_g2 _)
      }
      else {
        (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _))
        .joinNCoGroupLocal(sqlContext, models, data, method, true, test_local_g1 _, test_local_g2 _)
      }
      
      val ret1 = ret.persist(StorageLevel.MEMORY_AND_DISK)
      ret1.count
      System.out.println("Total execution time for testLDA(" + method + "):" + (System.nanoTime() - start)*(1e-9) + " sec.\n")
      ret1
    }
    
    def preprocessGMM(line: String): Data2 = {
      val rand = new Random()
      val splits = line.split('|')
      val text = splits(1).drop(1).dropRight(1).split(',')
      val point = Array.ofDim[Double](text.length)
      for (i <- 0 until text.length) point(i) = java.lang.Double.parseDouble(text(i))
      val membership = rand.nextInt(GMMData2.C)
      new GMMData2(membership, point).asInstanceOf[Data2]
    }
    
    // val data = sc.textFile(initialData).map(t.preprocessGMM)
    def testGMM(sc:SparkContext, sqlContext:SQLContext, method:String, data:RDD[Data2]) = {
      val start = System.nanoTime()
      
      val models = sc.parallelize(0 to (GMMData2.C-1)).map(x => new GMMModel2(x).asInstanceOf[Model2])
      def test_B_i_data_hash(d:Data2) = 1
      def test_B_i_model_hash(m:Model2) = 1
      def test_B_i(m:Model2, d:Data2):Boolean = {
        val m1: GMMModel2 = m.asInstanceOf[GMMModel2]  
        val d1: GMMData2 = d.asInstanceOf[GMMData2]
        if(m1.clusterID == d1.membership) true else false
      }
      def test_g(m:Model2)(d:Data2):Iterable[Delta2] = {
        m.asInstanceOf[GMMModel2].process()
        d.asInstanceOf[GMMData2].process(m)
      }
      def test_local_g1(m:Model2): Object = {
        m.asInstanceOf[GMMModel2].process()
      }
      
      def test_local_g2(del1:Object, d:Data2): Iterable[Delta2] = {
        val m1 = del1.asInstanceOf[Model2]
        d.asInstanceOf[GMMData2].process(m1)
      }
      def test_agg(it:Iterable[Delta2], d:Data2):Output2 = {
        val ret = new GMMOutput2();
        for (d <- it) {
          ret.addTuple(d.asInstanceOf[GMMDelta2])
        }
        ret
      }
      val ret = if(method.compareToIgnoreCase("naive") == 0 || method.compareToIgnoreCase("simulated-local") == 0) {
        (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _))
        .joinNCoGroup(sqlContext, models, data, method, false, test_g _)
      }
      else if(method.compareToIgnoreCase("local") == 0) {
        (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _))
        .joinNCoGroupLocal(sqlContext, models, data, method, false, test_local_g1 _, test_local_g2 _)
      }
      else {
        (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _))
        .joinNCoGroupLocal(sqlContext, models, data, method, true, test_local_g1 _, test_local_g2 _)
      }
      
      val ret1 = ret.persist(StorageLevel.MEMORY_AND_DISK)
      ret1.count
      System.out.println("Total execution time for testGMM(" + method + "):" + (System.nanoTime() - start)*(1e-9) + " sec.\n")
      ret1
    }
    
    def preprocessLR(line: String): Data2 = {
      val splits = line.split('|')
      val text = splits(1).drop(1).dropRight(1).split(',')
      val point = Array.ofDim[Double](text.length)
      for (i <- 0 until text.length) point(i) = java.lang.Double.parseDouble(text(i))
      new LRData2(java.lang.Integer.parseInt(splits(0)), point, java.lang.Double.parseDouble(splits(2))).asInstanceOf[Data2]
    }
    
		// val data = sc.textFile(initialData).map(t.preprocessLR)
    def testLR(sc:SparkContext, sqlContext:SQLContext, method:String, data:RDD[Data2]) = {
      val start = System.nanoTime()
      val models = sc.parallelize(0 to 0).map(x => new LRModel2().asInstanceOf[Model2])
      def test_B_i_data_hash(d:Data2) = 1
      def test_B_i_model_hash(m:Model2) = 1
      def test_B_i(m:Model2, d:Data2):Boolean = {
        true
      }
      def test_g(m:Model2)(d:Data2):Iterable[Delta2] = {
        m.asInstanceOf[LRModel2].process()
        d.asInstanceOf[LRData2].process(m)
      }
      def test_local_g1(m:Model2): Object = {
        m.asInstanceOf[LRModel2].process()
      }
      
      def test_local_g2(del1:Object, d:Data2): Iterable[Delta2] = {
        val m1 = del1.asInstanceOf[Model2]
        d.asInstanceOf[LRData2].process(m1)
      }
      def test_agg(it:Iterable[Delta2], d:Data2):Output2 = {
        val ret = new LROutput2();
        for (d <- it) {
          ret.addTuple(d.asInstanceOf[LRDelta2])
        }
        ret
      }
      val ret = if(method.compareToIgnoreCase("naive") == 0 || method.compareToIgnoreCase("simulated-local") == 0) {
        (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _))
        .joinNCoGroup(sqlContext, models, data, method, false, test_g _)
      }
      else if(method.compareToIgnoreCase("local") == 0) {
        (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _))
        .joinNCoGroupLocal(sqlContext, models, data, method, false, test_local_g1 _, test_local_g2 _)
      }
      else {
        (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _, test_B_i _, test_agg _))
        .joinNCoGroupLocal(sqlContext, models, data, method, true, test_local_g1 _, test_local_g2 _)
      }
      
      val ret1 = ret.persist(StorageLevel.MEMORY_AND_DISK)
      ret1.count
      System.out.println("Total execution time for testLR(" + method + "):" + (System.nanoTime() - start)*(1e-9) + " sec.\n")
      ret1
    }
    
}

class MLJoin2(
        // For natural join: 
        B_i_model_hash: Model2 => Long,
        B_i_data_hash: Data2 => Long,
        B_i: (Model2, Data2) => Boolean,
        agg: (Iterable[Delta2], Data2) => Output2) extends Logging with Serializable {
 
    def serialize(o:Object):Array[Byte] = {
      val start = System.nanoTime()
      val ret = SerializeUtils.serialize(o)
      Statistics.serializeTime.addAndGet(System.nanoTime() - start)
      Statistics.numSerialization.addAndGet(1)
      ret
     }
    def deserialize(b:Array[Byte]):Object = {
      val start = System.nanoTime()
      val ret = SerializeUtils.deserialize(b)
      Statistics.deserializeTime.addAndGet(System.nanoTime() - start)
      Statistics.numDeSerialization.addAndGet(1)
      ret
    }
    def serialized_B_i(m: Array[Byte], d:Array[Byte]): Boolean = {
      val start = System.nanoTime()
      val ret = B_i(deserialize(m).asInstanceOf[Model2], deserialize(d).asInstanceOf[Data2])
      Statistics.serialized_B_i.addAndGet(System.nanoTime() - start)
      ret
    }
    def serialized_simulated_local_B_i(m: Array[Byte], d:Array[Byte]): Boolean = {
      val start = System.nanoTime()
      val ret = B_i(deserialize(m).asInstanceOf[(Model2, Data2 => Iterable[Delta2])]._1, deserialize(d).asInstanceOf[Data2])
      Statistics.serialized_simulated_local_B_i.addAndGet(System.nanoTime() - start)
      ret
    }
    def serialized_local_B_i(m: Array[Byte], d:Array[Byte]): Boolean = {
      val start = System.nanoTime()
      val ret = B_i(deserialize(m).asInstanceOf[(Model2, Object)]._1, deserialize(d).asInstanceOf[Data2])
      Statistics.serialized_local_B_i.addAndGet(System.nanoTime() - start)
      ret
    }
    
    def joinNCoGroup(sqlContext:SQLContext, models :RDD[Model2], data :RDD[Data2], method:String, applyHash:Boolean,
        g: Model2 => Data2 => Iterable[Delta2]): RDD[Output2] = {
      // Step 1: Seeding
      seeding(sqlContext, data)
      
      // Step 2: Preparing parameters
      prepareParameters(sqlContext, models, method, g, null, null)
      
      if(method.compareToIgnoreCase("naive") == 0) {
        sqlContext.udf.register("B_i", serialized_B_i _)
      }
      else if(method.compareToIgnoreCase("simulated-local") == 0) {
        sqlContext.udf.register("B_i", serialized_simulated_local_B_i _)
      }
      else if(method.compareToIgnoreCase("local") == 0) {
        throw new RuntimeException("Unsupported method:" + method + ". If you want to apply local method, please use joinNCoGroupLocal")
      }
      else {
        throw new RuntimeException("Unsupported method:" + method)
      }
      
      // Step 3: Spark SQL join n cogroup
      doSparkSQLJoinNCoGroup(sqlContext, method, applyHash, g, null, null)
    }
    
    def joinNCoGroupLocal(sqlContext:SQLContext, models :RDD[Model2], data :RDD[Data2], method:String, applyHash:Boolean,
        g1: Model2 => Object, g2: (Object, Data2) => Iterable[Delta2]): RDD[Output2] = {
      // Step 1: Seeding
      seeding(sqlContext, data)
      
      // Step 2: Preparing parameters
      prepareParameters(sqlContext, models, method, null, g1, g2)
      
      if(method.compareToIgnoreCase("local") == 0 || method.compareToIgnoreCase("global") == 0) {
        sqlContext.udf.register("B_i", serialized_local_B_i _)
      }
      else if(method.compareToIgnoreCase("naive") == 0 || method.compareToIgnoreCase("simulated-local") == 0) {
        throw new RuntimeException("Unsupported method:" + method + ". If you want to apply naive and simulated-local method, please use joinNCoGroup")
      }
      else {
        throw new RuntimeException("Unsupported method:" + method)
      }
      
      // Step 3: Spark SQL join n cogroup
      doSparkSQLJoinNCoGroup(sqlContext, method, applyHash, null, g1, g2)
    }
    
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
    
    def convertSimulatedLocalModelToDF(sqlContext:SQLContext, param :RDD[(Long, (Model2, Data2 => Iterable[Delta2]))], 
        B_i_model_hash: Model2 => Long): DataFrame = {
      // Instead of UDT which will get removed in Spark 2.0 https://issues.apache.org/jira/browse/SPARK-14155
      val modelType:StructType = new StructType(
          Array(new StructField("id", LongType, nullable=false), 
            new StructField("hash", LongType, nullable=false), 
            new StructField("model", BinaryType, nullable=false)))
      sqlContext.createDataFrame(param.map( x => (new CustomRow(x._1,  B_i_model_hash(x._2._1), serialize(x._2))).asInstanceOf[Row] ), modelType)
    }
    
    def convertLocalModelToDF(sqlContext:SQLContext, param :RDD[(Long, (Model2, Object))], 
        B_i_model_hash: Model2 => Long): DataFrame = {
      // Instead of UDT which will get removed in Spark 2.0 https://issues.apache.org/jira/browse/SPARK-14155
      val modelType:StructType = new StructType(
          Array(new StructField("id", LongType, nullable=false), 
            new StructField("hash", LongType, nullable=false), 
            new StructField("model", BinaryType, nullable=false)))
      sqlContext.createDataFrame(param.map( x => (new CustomRow(x._1,  B_i_model_hash(x._2._1), serialize(x._2))).asInstanceOf[Row] ), modelType)
    }
    
    def seeding(sqlContext:SQLContext, data :RDD[Data2]):Unit = {
      val start = System.nanoTime()
      val X :RDD[(Long, Data2)] = data.zipWithIndex().map(x => (x._2, x._1.asInstanceOf[Data2])).persist(StorageLevel.MEMORY_AND_DISK)
      
      val count1 = X.count
      val XDF:DataFrame = convertDataToDF(sqlContext, X, B_i_data_hash)             
      XDF.registerTempTable("Data")
      Statistics.seeding.addAndGet(System.nanoTime() - start)
    }
    
    def prepareParameters(sqlContext:SQLContext, models :RDD[Model2], method:String,
        g: Model2 => Data2 => Iterable[Delta2], g1: Model2 => Object, g2: (Object, Data2) => Iterable[Delta2]): Unit = {
      val start = System.nanoTime()
      if(method.compareToIgnoreCase("naive") == 0) {
        // Step 2: Preparing parameters
        val param :RDD[(Long, Model2)] = models.zipWithIndex().map(x => (x._2, x._1))
        val paramDF:DataFrame = convertModelToDF(sqlContext, param, B_i_model_hash)
        paramDF.registerTempTable("Model")
      }
      else if(method.compareToIgnoreCase("simulated-local") == 0) {
        // Step 2: Preparing parameters
        val param :RDD[(Long, (Model2, Data2 => Iterable[Delta2]))] = models
            .zipWithIndex()
            .map(x => (x._2, (x._1, g(x._1))))
        val paramDF:DataFrame = convertSimulatedLocalModelToDF(sqlContext, param, B_i_model_hash)
        paramDF.registerTempTable("Model")
      }
      else if(method.compareToIgnoreCase("local") == 0 || method.compareToIgnoreCase("global") == 0) {
        // Step 2: Preparing parameters
        val param :RDD[(Long, (Model2, Object))] = models
            .zipWithIndex()
            .map(x => (x._2, (x._1, g1(x._1))))
        val paramDF:DataFrame = convertLocalModelToDF(sqlContext, param, B_i_model_hash)
        paramDF.registerTempTable("Model")
      }
      else {
        throw new RuntimeException("Unsupported method:" + method)
      }
      Statistics.prepareParameters.addAndGet(System.nanoTime() - start)
    }
    
    def doSparkSQLJoinNCoGroup(sqlContext:SQLContext, method:String, applyHash:Boolean,
        g: Model2 => Data2 => Iterable[Delta2],
        g1: Model2 => Object, g2: (Object, Data2) => Iterable[Delta2]): RDD[Output2] = {
      val start = System.nanoTime()
      val sqlStr:String = if(applyHash)
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
        if(g == null) {
          throw new RuntimeException("The function g cannot be null");
        }
        
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
                           var start = System.nanoTime()
                           val ret1 = models.map(g(_)(d)).map(agg(_, d)).map((id, _))
                           Statistics.groupByKeyFlatMapApplication.addAndGet(System.nanoTime() - start)                     
                           ret1
                           }  
                         ).values //.sortByKey().values
      }
      else if(method.compareToIgnoreCase("simulated-local") == 0) {
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
                           var start = System.nanoTime()
                           val ret1 = models.map(_._2(d)).map(agg(_, d)).map((id, _))
                           Statistics.groupByKeyFlatMapApplication.addAndGet(System.nanoTime() - start)
                           ret1
                         }).values //.sortByKey().values
      }
      else if(method.compareToIgnoreCase("local") == 0 || method.compareToIgnoreCase("global") == 0) {
        
        if(g2 == null) {
          throw new RuntimeException("The function g2 cannot be null");
        }
        // Join
        val temp:RDD[(Long, (Model2, Object), Data2)] = sqlContext.sql(sqlStr).rdd
                      .map(r => (r.get(0).asInstanceOf[Long],
                           deserialize(r.get(1).asInstanceOf[Array[Byte]]).asInstanceOf[(Model2, Object)],
                           deserialize(r.get(2).asInstanceOf[Array[Byte]]).asInstanceOf[Data2]))
        
        // Step 3: Cogroup
        // Step 4: UDF invocation and final output assembly
        // TODO: Jacob: Please double check final output assembly
        ret = temp.map(x => ((x._1, x._3), x._2))
                         .groupByKey()
                         .flatMap(x => {
                           val d:Data2 = x._1._2
                           val id:Long = x._1._1
                           val models:Iterable[(Model2, Object)] = x._2
                           var start = System.nanoTime()
                           val ret1 = models.map(y => g2(y._2, d)).map(agg(_, d)).map((id, _))
                           Statistics.groupByKeyFlatMapApplication.addAndGet(System.nanoTime() - start)
                           ret1
                         }).values //.sortByKey().values
      }
      else {
        throw new RuntimeException("Unsupported method:" + method)
      }
      Statistics.doSparkSQLJoinNCoGroup.addAndGet(System.nanoTime() - start)
      ret
    }
    
}