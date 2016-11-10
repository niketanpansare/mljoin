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

class CustomRow(id: Long, hash: Long, foo: Array[Byte]) extends Row with Serializable {
  override def copy() = (new CustomRow(id, hash, foo)).asInstanceOf[Row]
  override def get(i: Int) = if (i == 0) id else if (i == 1) hash else foo
  override def length = 3
  override def apply(i: Int): Any = if (i == 0) id else if (i == 1) hash else foo
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

  def generateSampleData(sc: SparkContext): RDD[Data2] = sc.parallelize(1 to 10).map(x => { val r = new java.util.Random(); new TestData2(r.nextInt, r.nextInt).asInstanceOf[Data2] })
  def test1(sc: SparkContext, sqlContext: SQLContext, method: String, data: RDD[Data2]) = {
    val models = sc.parallelize(1 to 10).map(x => { val r = new java.util.Random(); new TestModel2(r.nextInt, r.nextInt).asInstanceOf[Model2] })
    def test_B_i_data_hash(d: Data2) = 1
    def test_B_i_model_hash(m: Model2) = 1
    def test_B_i(m: Model2, d: Data2): Boolean = {
      val m1: TestModel2 = m.asInstanceOf[TestModel2]
      val d1: TestData2 = d.asInstanceOf[TestData2]
      // Some random criteria
      if (m1.val1 > d1.val1) true else false
    }
    def test_g1(m: Model2) = {
      System.out.println(">" + m.asInstanceOf[TestModel2].val1)
      new TestDelta2(m.asInstanceOf[TestModel2].val1, m.asInstanceOf[TestModel2].val2)
    }
    def test_g(m: Model2)(d: Data2): Iterable[Delta2] = {
      val ret = new ArrayList[Delta2]
      val del = test_g1(m)
      System.out.println("-" + del.val1)
      ret.add(new TestDelta2(del.val1 + d.asInstanceOf[TestData2].val1,
        del.val2 + d.asInstanceOf[TestData2].val2).asInstanceOf[TestDelta2])
      ret
    }
    def test_local_g1(m: Model2): Object = {
      System.out.println(">" + m.asInstanceOf[TestModel2].val1)
      new TestDelta2(m.asInstanceOf[TestModel2].val1, m.asInstanceOf[TestModel2].val2)
    }

    def test_local_g2(del1: Object, d: Data2): Iterable[Delta2] = {
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
    def test_agg(it: Iterable[Delta2], d: Data2): Output2 = {
      new TestOutput2(d.asInstanceOf[TestData2].val1 - it.head.asInstanceOf[TestDelta2].val1,
        d.asInstanceOf[TestData2].val2 - it.head.asInstanceOf[TestDelta2].val2)
    }
    if (method.compareToIgnoreCase("naive") == 0 || method.compareToIgnoreCase("simulated-local") == 0) {
      (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _,
        test_B_i _,
        test_agg _))
        .joinNCoGroup(sqlContext, models, data, method, false, test_g _)
    } else if (method.compareToIgnoreCase("local") == 0) {
      (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _,
        test_B_i _,
        test_agg _))
        .joinNCoGroupLocal(sqlContext, models, data, method, false, test_local_g1 _, test_local_g2 _)
    } else if (method.compareToIgnoreCase("global") == 0) {
      (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _,
        test_B_i _,
        test_agg _))
        .joinNCoGroupLocal(sqlContext, models, data, method, true, test_local_g1 _, test_local_g2 _)
    } else {
      throw new RuntimeException("The method " + method + " is not supported.")
    }
  }

  def preprocessLDA(line: String): Data2 = {
    val splits = line.split('|')
    val text = splits(2).drop(1).dropRight(1).split(',')
    val wordsInDoc = Array.ofDim[Int](text.length / 2)
    val wordCounts = Array.ofDim[Int](text.length / 2)
    for (i <- 0 until text.length / 2) {
      wordsInDoc(i) = java.lang.Integer.parseInt(text(2 * i).trim)
      wordCounts(i) = java.lang.Integer.parseInt(text(2 * i + 1).trim)
    }
    new LDAData2(java.lang.Integer.parseInt(splits(0)), java.lang.Integer.parseInt(splits(1)), wordsInDoc, wordCounts).asInstanceOf[Data2]
  }

  // val data = sc.textFile(initialData).map(t.preprocessLDA)
  def testLDA(sc: SparkContext, sqlContext: SQLContext, method: String, data: RDD[Data2]) = {
    val start = System.nanoTime()
    val models = sc.parallelize(0 to (LDAData2.WB - 1), data.getNumPartitions).map(x => new LDAModel2(x).asInstanceOf[Model2])
    def test_B_i_data_hash(d: Data2) = d.asInstanceOf[LDAData2].wordBlockID
    def test_B_i_model_hash(m: Model2) = m.asInstanceOf[LDAModel2].wordBlockID
    def test_B_i(m: Model2, d: Data2): Boolean = {
      val m1: LDAModel2 = m.asInstanceOf[LDAModel2]
      val d1: LDAData2 = d.asInstanceOf[LDAData2]
      if (m1.wordBlockID == d1.wordBlockID) true else false
    }
    def test_g(m: Model2)(d: Data2): Iterable[Delta2] = {
      m.asInstanceOf[LDAModel2].process()
      d.asInstanceOf[LDAData2].process(m)
    }
    def test_local_g1(m: Model2): Object = {
      m.asInstanceOf[LDAModel2].process()
    }

    def test_local_g2(del1: Object, d: Data2): Iterable[Delta2] = {
      val m1 = del1.asInstanceOf[Model2]
      d.asInstanceOf[LDAData2].process(m1)
    }
    def test_agg(it: Iterable[Delta2], d: Data2): Output2 = {
      val ret = new LDAOutput2();
      for (d <- it) {
        ret.addTuple(d.asInstanceOf[LDADelta2a])
      }
      ret
    }
    val ret = if (method.compareToIgnoreCase("naive") == 0 || method.compareToIgnoreCase("simulated-local") == 0) {
      (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _,
        test_B_i _,
        test_agg _))
        .joinNCoGroupNew(sqlContext, models, data, method, true, test_g _)
    } else if (method.compareToIgnoreCase("local") == 0) {
      (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _,
        test_B_i _,
        test_agg _))
        .joinNCoGroupLocalNew(sqlContext, models, data, method, true, test_local_g1 _, test_local_g2 _)
    } else if (method.compareToIgnoreCase("global") == 0) {
      (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _,
        test_B_i _,
        test_agg _))
        .joinNCoGroupLocalNew(sqlContext, models, data, method, true, test_local_g1 _, test_local_g2 _)
    } else {
      throw new RuntimeException("The method " + method + " is not supported.")
    }

    val ret1 = ret.persist(StorageLevel.MEMORY_AND_DISK)
    ret1.count
    System.out.println("Total execution time for testLDA(" + method + "):" + (System.nanoTime() - start) * (1e-9) + " sec.\n")
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
  def testGMM(sc: SparkContext, sqlContext: SQLContext, method: String, data: RDD[Data2]) = {
    val start = System.nanoTime()

    val models = sc.parallelize(0 to (GMMData2.C - 1), data.getNumPartitions).map(x => new GMMModel2(x).asInstanceOf[Model2])
    def test_B_i_data_hash(d: Data2) = d.asInstanceOf[GMMData2].membership
    def test_B_i_model_hash(m: Model2) = m.asInstanceOf[GMMModel2].clusterID
    def test_B_i(m: Model2, d: Data2): Boolean = {
      val m1: GMMModel2 = m.asInstanceOf[GMMModel2]
      val d1: GMMData2 = d.asInstanceOf[GMMData2]
      if (m1.clusterID == d1.membership) true else false
    }
    def test_g(m: Model2)(d: Data2): Iterable[Delta2] = {
      m.asInstanceOf[GMMModel2].process()
      d.asInstanceOf[GMMData2].process(m)
    }
    def test_local_g1(m: Model2): Object = {
      m.asInstanceOf[GMMModel2].process()
    }

    def test_local_g2(del1: Object, d: Data2): Iterable[Delta2] = {
      val m1 = del1.asInstanceOf[Model2]
      d.asInstanceOf[GMMData2].process(m1)
    }
    def test_agg(it: Iterable[Delta2], d: Data2): Output2 = {
      val ret = new GMMOutput2();
      for (d <- it) {
        ret.addTuple(d.asInstanceOf[GMMDelta2])
      }
      ret
    }
    val ret = if (method.compareToIgnoreCase("naive") == 0 || method.compareToIgnoreCase("simulated-local") == 0) {
      (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _,
        test_B_i _,
        test_agg _))
        .joinNCoGroupNew(sqlContext, models, data, method, true, test_g _)
    } else if (method.compareToIgnoreCase("local") == 0) {
      (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _,
        test_B_i _,
        test_agg _))
        .joinNCoGroupLocalNew(sqlContext, models, data, method, true, test_local_g1 _, test_local_g2 _)
    } else if (method.compareToIgnoreCase("global") == 0) {
      (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _,
        test_B_i _,
        test_agg _))
        .joinNCoGroupLocalNew(sqlContext, models, data, method, true, test_local_g1 _, test_local_g2 _)
    } else {
      throw new RuntimeException("The method " + method + " is not supported.")
    }

    val ret1 = ret.persist(StorageLevel.MEMORY_AND_DISK)
    ret1.count
    System.out.println("Total execution time for testGMM(" + method + "):" + (System.nanoTime() - start) * (1e-9) + " sec.\n")
    ret1
  }

  def preprocessLR(line: String): Data2 = {
    val splits = line.split('|')
    val text = splits(1).drop(1).dropRight(1).split(',')
    val point = Array.ofDim[Double](text.length)
    for (i <- 0 until text.length) point(i) = if (text(i) == null || text(i).trim.isEmpty || text(i).indexOf('e') != text(i).lastIndexOf('e')) 0.0 else java.lang.Double.parseDouble(text(i))
    new LRData2(java.lang.Integer.parseInt(splits(0)), point, java.lang.Double.parseDouble(splits(2))).asInstanceOf[Data2]
  }

  // val data = sc.textFile(initialData).map(t.preprocessLR)
  def testLR(sc: SparkContext, sqlContext: SQLContext, method: String, data: RDD[Data2]) = {
    val start = System.nanoTime()
    val models = sc.parallelize(0 to 0).map(x => new LRModel2().asInstanceOf[Model2])
    def test_B_i_data_hash(d: Data2) = 1
    def test_B_i_model_hash(m: Model2) = 1
    def test_B_i(m: Model2, d: Data2): Boolean = {
      true
    }
    def test_g(m: Model2)(d: Data2): Iterable[Delta2] = {
      m.asInstanceOf[LRModel2].process()
      d.asInstanceOf[LRData2].process(m)
    }
    def test_local_g1(m: Model2): Object = {
      m.asInstanceOf[LRModel2].process()
    }

    def test_local_g2(del1: Object, d: Data2): Iterable[Delta2] = {
      val m1 = del1.asInstanceOf[Model2]
      d.asInstanceOf[LRData2].process(m1)
    }
    def test_agg(it: Iterable[Delta2], d: Data2): Output2 = {
      val ret = new LROutput2();
      for (d <- it) {
        ret.addTuple(d.asInstanceOf[LRDelta2])
      }
      ret
    }
    val ret = if (method.compareToIgnoreCase("naive") == 0 || method.compareToIgnoreCase("simulated-local") == 0) {
      (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _,
        test_B_i _,
        test_agg _))
        .joinNCoGroupNew(sqlContext, models, data, method, true, test_g _)
    } else if (method.compareToIgnoreCase("local") == 0) {
      (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _,
        test_B_i _,
        test_agg _))
        .joinNCoGroupLocalNew(sqlContext, models, data, method, true, test_local_g1 _, test_local_g2 _)
    } else if (method.compareToIgnoreCase("global") == 0) {
      (new MLJoin2(test_B_i_model_hash _, test_B_i_data_hash _,
        test_B_i _,
        test_agg _))
        .joinNCoGroupLocalNew(sqlContext, models, data, method, true, test_local_g1 _, test_local_g2 _)
    } else {
      throw new RuntimeException("The method " + method + " is not supported.")
    }

    val ret1 = ret.persist(StorageLevel.MEMORY_AND_DISK)
    ret1.count
    System.out.println("Total execution time for testLR(" + method + "):" + (System.nanoTime() - start) * (1e-9) + " sec.\n")
    ret1
  }

}

class MLJoin2(
    // For natural join: 
    B_i_model_hash: Model2 => Long,
    B_i_data_hash: Data2 => Long,
    B_i: (Model2, Data2) => Boolean,
    agg: (Iterable[Delta2], Data2) => Output2) extends Logging with Serializable {

  def serialize(o: Object): Array[Byte] = {
    val start = System.nanoTime()
    val ret = SerializeUtils.serialize(o)
    Statistics.serializeTime.addAndGet(System.nanoTime() - start)
    Statistics.numSerialization.addAndGet(1)
    ret
  }
  def deserialize(b: Array[Byte]): Object = {
    val start = System.nanoTime()
    val ret = SerializeUtils.deserialize(b)
    Statistics.deserializeTime.addAndGet(System.nanoTime() - start)
    Statistics.numDeSerialization.addAndGet(1)
    ret
  }
  def serialized_B_i(m: Array[Byte], d: Array[Byte]): Boolean = {
    val start = System.nanoTime()
    val ret = B_i(deserialize(m).asInstanceOf[Model2], deserialize(d).asInstanceOf[Data2])
    Statistics.serialized_B_i.addAndGet(System.nanoTime() - start)
    ret
  }
  def serialized_simulated_local_B_i(m: Array[Byte], d: Array[Byte]): Boolean = {
    val start = System.nanoTime()
    val ret = B_i(deserialize(m).asInstanceOf[(Model2, Data2 => Iterable[Delta2])]._1, deserialize(d).asInstanceOf[Data2])
    Statistics.serialized_simulated_local_B_i.addAndGet(System.nanoTime() - start)
    ret
  }
  def serialized_local_B_i(m: Array[Byte], d: Array[Byte]): Boolean = {
    val start = System.nanoTime()
    val ret = B_i(deserialize(m).asInstanceOf[(Model2, Object)]._1, deserialize(d).asInstanceOf[Data2])
    Statistics.serialized_local_B_i.addAndGet(System.nanoTime() - start)
    ret
  }

  def joinNCoGroupNew(sqlContext: SQLContext, models: RDD[Model2], data: RDD[Data2], method: String,
                      applyHash: Boolean,
                      g: Model2 => Data2 => Iterable[Delta2]): RDD[Output2] = {

    if (method.compareToIgnoreCase("naive") != 0) {
      throw new RuntimeException("The method is not supported:" + method)
    }
    val totalStart = System.nanoTime()
    val out = performAggregation(
      applyUDFNaive(
        mapSideJoinNaive(
          seedingNew(data, applyHash),
          broadcastNaiveModelNew(sqlContext, models, applyHash)), g))

    System.out.println("Total Time: " + (System.nanoTime() - totalStart) * (1e-9) + " sec.")
    printStatsNew()
    out
  }

  def seedingNew(data: RDD[Data2], applyHash: Boolean): RDD[(Long, Long, Data2)] = {
    val start = System.nanoTime()
    val data1: RDD[(Long, Long, Data2)] = data.zipWithIndex().map(x =>
      if (applyHash) (x._2, B_i_data_hash(x._1), x._1)
      else (x._2, 0L, x._1)) //.persist(StorageLevel.MEMORY_AND_DISK)
//    data1.count   
    seedingTime = (System.nanoTime() - start) * (1e-9)
    data1
  }

  def applyUDFNaive(joinedRDD: RDD[(Long, Data2, Model2)], g: Model2 => Data2 => Iterable[Delta2]): RDD[((Long, Data2), Iterable[Delta2])] = {
    val start = System.nanoTime()
    val ret = joinedRDD.map(x => {
      val id: Long = x._1
      val d: Data2 = x._2
      val m: Model2 = x._3
      ((id, d), g(m)(d))
    })
    ret.count
    applyUDFTime = (System.nanoTime() - start) * (1e-9)
    joinedRDD.unpersist()
    ret
  }

  def mapSideJoinNaive(data1: RDD[(Long, Long, Data2)], model1: Broadcast[Array[(Long, Model2)]]): RDD[(Long, Data2, Model2)] = {
    val start = System.nanoTime()
    val ret = data1.flatMap(x => {
      val m: Array[(Long, Model2)] = model1.value
      val id: Long = x._1
      val data_hash: Long = x._2
      val d: Data2 = x._3
      m.filter(_._1 == data_hash).map(m1 => (id, d, m1._2))
    })
    ret.count
    joinTime = (System.nanoTime() - start) * (1e-9)
    data1.unpersist()
    ret
  }

  def broadcastNaiveModelNew(sqlContext: SQLContext, models: RDD[Model2], applyHash: Boolean): Broadcast[Array[(Long, Model2)]] = {
    val start = System.nanoTime()
    val modelWithHash: Array[(Long, Model2)] = models.map(x => if (applyHash) (B_i_model_hash(x), x) else (0L, x)).collect
    val model1 = sqlContext.sparkContext.broadcast(modelWithHash)
    globalModelBroadcastTime = (System.nanoTime() - start) * (1e-9)
    model1
  }

  def mapSideJoinLocal(data1: RDD[(Long, Long, Data2)], model1: Broadcast[Array[(Long, Object)]]): RDD[(Long, Data2, Object)] = {
    val start = System.nanoTime()
    val ret = data1.flatMap(x => {
      val m: Array[(Long, Object)] = model1.value
      val id: Long = x._1
      val data_hash: Long = x._2
      val d: Data2 = x._3
      m.filter(_._1 == data_hash).map(m1 => (id, d, m1._2))
    })
//    ret.count
    joinTime = (System.nanoTime() - start) * (1e-9)
//    data1.unpersist()
    ret
  }

  def applyUDFLocal(joinedRDD: RDD[(Long, Data2, Object)], g2: (Object, Data2) => Iterable[Delta2]): RDD[((Long, Data2), Iterable[Delta2])] = {
    val start = System.nanoTime()
    val ret = joinedRDD.map(x => {
      val id: Long = x._1
      val d: Data2 = x._2
      val m: Object = x._3
      ((id, d), g2(m, d))
    })
    //      ret.count
    applyUDFTime = (System.nanoTime() - start) * (1e-9)
    //      joinedRDD.unpersist()
    ret
  }

  def broadcastLocalModelNew(sqlContext: SQLContext, models: RDD[Model2], applyHash: Boolean, g1: Model2 => Object): Broadcast[Array[(Long, Object)]] = {
    val start = System.nanoTime()
    val modelWithHash: Array[(Long, Object)] = models.map(x => if (applyHash) (B_i_model_hash(x), g1(x)) else (0L, g1(x))).collect
    val model1 = sqlContext.sparkContext.broadcast(modelWithHash)
    globalModelBroadcastTime = (System.nanoTime() - start) * (1e-9)
    model1
  }

  def performAggregation(in: RDD[((Long, Data2), Iterable[Delta2])]): RDD[Output2] = {
    val start = System.nanoTime()
    val ret = in.reduceByKey((it1, it2) => it1 ++ it2)
      .map(x => {
        val id: Long = x._1._1
        val d: Data2 = x._1._2
        val itDelta: Iterable[Delta2] = x._2
        (id, agg(itDelta, d))
      }).values//.persist(StorageLevel.MEMORY_AND_DISK)
    ret.count //.sortByKey().values
    aggregationTime = (System.nanoTime() - start) * (1e-9)
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

  def joinNCoGroupLocalNew(sqlContext: SQLContext, models: RDD[Model2], data: RDD[Data2], method: String,
                           applyHash: Boolean,
                           g1: Model2 => Object, g2: (Object, Data2) => Iterable[Delta2]): RDD[Output2] = {

    val totalStart = System.nanoTime()
    val out =
      if (method.compareToIgnoreCase("local") == 0) {
        performAggregation(
          applyUDFLocal(
            mapSideJoinLocal(
              seedingNew(data, applyHash),
              broadcastLocalModelNew(sqlContext, models, applyHash, g1)), g2))
      } else if (method.compareToIgnoreCase("global") == 0) {
        // For now keeping this static as we don't know cardinality of hash function
        // Also we need to ensure that we donot reduce the parallelism
        numPartitions = sqlContext.sparkContext.defaultParallelism // Math.min( models.getNumPartitions, data.getNumPartitions ) 

        val t1 = System.nanoTime()
        val repartitionedModel: RDD[(Long, Object)] = models.map(x =>
          if (applyHash) (x, B_i_model_hash(x))
          else throw new RuntimeException("Expected applyHash"))
          .partitionBy(new GlobalModelPartitioner(numPartitions, B_i_model_hash))
          .map(_.swap)
          .map(x => (x._1, g1(x._2)))
          .persist(StorageLevel.MEMORY_AND_DISK)
        repartitionedModel.count
        globalModelShuffleTime = (System.nanoTime() - t1) * (1e-9)

        val dat1 = seedingNew(data, applyHash)

        val t2 = System.nanoTime()
        val repartitionedData: RDD[(Long, (Long, Data2))] = dat1.map(x =>
          (x._3, (x._2, x._1)))
          .partitionBy(new GlobalDataPartitioner(numPartitions, B_i_data_hash))
          .map(x => (x._2._1, (x._2._2, x._1)))
          .persist(StorageLevel.MEMORY_AND_DISK)
        repartitionedData.count
        globalDataShuffleTime = (System.nanoTime() - t2) * (1e-9)

        val t3 = System.nanoTime()
        val joinedRDD: RDD[(Long, Data2, Object)] = repartitionedData.join(repartitionedModel)
          .map(y => {
            val id: Long = y._2._1._1
            val d: Data2 = y._2._1._2
            val m: Object = y._2._2
            (id, d, m)
          }) //.persist(StorageLevel.MEMORY_AND_DISK)
        //          joinedRDD.count
        joinTime = (System.nanoTime() - t3) * (1e-9)

        performAggregation(
          applyUDFLocal(joinedRDD, g2))

      } else {
        throw new RuntimeException("The method is not supported:" + method)
      }
    System.out.println("Total Time: " + (System.nanoTime() - totalStart) * (1e-9) + " sec.")
    printStatsNew()
    out
  }

  var seedingTime: Double = 0
  var joinTime: Double = 0
  var applyUDFTime: Double = 0
  var aggregationTime: Double = 0
  var globalModelBroadcastTime: Double = 0

  var globalModelShuffleTime: Double = 0
  var globalDataShuffleTime: Double = 0

  def joinNCoGroup(sqlContext: SQLContext, models: RDD[Model2], data: RDD[Data2], method: String,
                   applyHash: Boolean,
                   g: Model2 => Data2 => Iterable[Delta2]): RDD[Output2] = {
    // Step 1: Seeding
    seeding(sqlContext, data, method)

    // Step 2: Preparing parameters
    prepareParameters(sqlContext, models, method, g, null, null)

    if (method.compareToIgnoreCase("naive") == 0) {
      sqlContext.udf.register("B_i", serialized_B_i _)
    } else if (method.compareToIgnoreCase("simulated-local") == 0) {
      sqlContext.udf.register("B_i", serialized_simulated_local_B_i _)
    } else if (method.compareToIgnoreCase("local") == 0) {
      throw new RuntimeException("Unsupported method:" + method + ". If you want to apply local method, please use joinNCoGroupLocal")
    } else {
      throw new RuntimeException("Unsupported method:" + method)
    }

    // Step 3: Spark SQL join n cogroup
    doSparkSQLJoinNCoGroup(sqlContext, method, applyHash, g, null, null, data.getNumPartitions)
  }

  def joinNCoGroupLocal(sqlContext: SQLContext, models: RDD[Model2], data: RDD[Data2], method: String,
                        applyHash: Boolean,
                        g1: Model2 => Object, g2: (Object, Data2) => Iterable[Delta2]): RDD[Output2] = {
    // For now keeping this static as we don't know cardinality of hash function
    // Also we need to ensure that we donot reduce the parallelism
    numPartitions = sqlContext.sparkContext.defaultParallelism // Math.min( models.getNumPartitions, data.getNumPartitions ) 

    // Step 1: Seeding
    seeding(sqlContext, data, method)

    // Step 2: Preparing parameters
    prepareParameters(sqlContext, models, method, null, g1, g2)

    if (method.compareToIgnoreCase("local") == 0 || method.compareToIgnoreCase("global") == 0) {
      sqlContext.udf.register("B_i", serialized_local_B_i _)
    } else if (method.compareToIgnoreCase("naive") == 0 || method.compareToIgnoreCase("simulated-local") == 0) {
      throw new RuntimeException("Unsupported method:" + method + ". If you want to apply naive and simulated-local method, please use joinNCoGroup")
    } else {
      throw new RuntimeException("Unsupported method:" + method)
    }

    // Step 3: Spark SQL join n cogroup
    doSparkSQLJoinNCoGroup(sqlContext, method, applyHash, null, g1, g2, data.getNumPartitions)
  }

  def convertDataToDF(sqlContext: SQLContext, X: RDD[(Long, Data2)],
                      B_i_data_hash: Data2 => Long): DataFrame = {
    // Instead of UDT which will get removed in Spark 2.0 https://issues.apache.org/jira/browse/SPARK-14155
    val dataType: StructType = new StructType(
      Array(new StructField("id", LongType, nullable = false),
        new StructField("hash", LongType, nullable = false),
        new StructField("data", BinaryType, nullable = false)))
    sqlContext.createDataFrame(X.map(x => (new CustomRow(x._1, B_i_data_hash(x._2), serialize(x._2))).asInstanceOf[Row]), dataType)
  }

  def convertModelToDF(sqlContext: SQLContext, param: RDD[(Long, Model2)],
                       B_i_model_hash: Model2 => Long): DataFrame = {
    // Instead of UDT which will get removed in Spark 2.0 https://issues.apache.org/jira/browse/SPARK-14155
    val modelType: StructType = new StructType(
      Array(new StructField("id", LongType, nullable = false),
        new StructField("hash", LongType, nullable = false),
        new StructField("model", BinaryType, nullable = false)))
    sqlContext.createDataFrame(param.map(x => (new CustomRow(x._1, B_i_model_hash(x._2), serialize(x._2))).asInstanceOf[Row]), modelType)
  }

  def convertSimulatedLocalModelToDF(sqlContext: SQLContext, param: RDD[(Long, (Model2, Data2 => Iterable[Delta2]))],
                                     B_i_model_hash: Model2 => Long): DataFrame = {
    // Instead of UDT which will get removed in Spark 2.0 https://issues.apache.org/jira/browse/SPARK-14155
    val modelType: StructType = new StructType(
      Array(new StructField("id", LongType, nullable = false),
        new StructField("hash", LongType, nullable = false),
        new StructField("model", BinaryType, nullable = false)))
    sqlContext.createDataFrame(param.map(x => (new CustomRow(x._1, B_i_model_hash(x._2._1), serialize(x._2))).asInstanceOf[Row]), modelType)
  }

  def convertLocalModelToDF(sqlContext: SQLContext, param: RDD[(Long, (Model2, Object))],
                            B_i_model_hash: Model2 => Long): DataFrame = {
    // Instead of UDT which will get removed in Spark 2.0 https://issues.apache.org/jira/browse/SPARK-14155
    val modelType: StructType = new StructType(
      Array(new StructField("id", LongType, nullable = false),
        new StructField("hash", LongType, nullable = false),
        new StructField("model", BinaryType, nullable = false)))
    sqlContext.createDataFrame(param.map(x => (new CustomRow(x._1, B_i_model_hash(x._2._1), serialize(x._2))).asInstanceOf[Row]), modelType)
  }

  var numPartitions = -1

  def seeding(sqlContext: SQLContext, data: RDD[Data2], method: String): Unit = {
    val start = System.nanoTime()
    val X: RDD[(Long, Data2)] =
      (if (method.compareToIgnoreCase("global") == 0) {
        data.zipWithIndex().partitionBy(new GlobalDataPartitioner(numPartitions, B_i_data_hash))
      } else {
        data.zipWithIndex()
      }).map(x => (x._2, x._1.asInstanceOf[Data2])).persist(StorageLevel.MEMORY_AND_DISK)

    val count1 = X.count
    val XDF: DataFrame = convertDataToDF(sqlContext, X, B_i_data_hash)
    XDF.registerTempTable("Data")
    Statistics.seeding.addAndGet(System.nanoTime() - start)
  }

  def prepareParameters(sqlContext: SQLContext, models: RDD[Model2], method: String,
                        g: Model2 => Data2 => Iterable[Delta2], g1: Model2 => Object, g2: (Object, Data2) => Iterable[Delta2]): Unit = {
    val start = System.nanoTime()
    if (method.compareToIgnoreCase("naive") == 0) {
      // Step 2: Preparing parameters
      val param: RDD[(Long, Model2)] = models.zipWithIndex().map(x => (x._2, x._1))
      val paramDF: DataFrame = convertModelToDF(sqlContext, param, B_i_model_hash)
      paramDF.registerTempTable("Model")
    } else if (method.compareToIgnoreCase("simulated-local") == 0) {
      // Step 2: Preparing parameters
      val param: RDD[(Long, (Model2, Data2 => Iterable[Delta2]))] = models
        .zipWithIndex()
        .map(x => (x._2, (x._1, g(x._1))))
      val paramDF: DataFrame = convertSimulatedLocalModelToDF(sqlContext, param, B_i_model_hash)
      paramDF.registerTempTable("Model")
    } else if (method.compareToIgnoreCase("local") == 0) {
      // Step 2: Preparing parameters
      val param: RDD[(Long, (Model2, Object))] = models
        .zipWithIndex()
        .map(x => (x._2, (x._1, g1(x._1))))
      val paramDF: DataFrame = convertLocalModelToDF(sqlContext, param, B_i_model_hash)
      paramDF.registerTempTable("Model")
    } else if (method.compareToIgnoreCase("global") == 0) {
      // Step 2: Preparing parameters
      val repartitionedModel = models.zipWithIndex()
        .partitionBy(new GlobalModelPartitioner(numPartitions, B_i_model_hash))
        .persist(StorageLevel.MEMORY_AND_DISK)
      repartitionedModel.count

      val param: RDD[(Long, (Model2, Object))] = repartitionedModel
        .map(x => (x._2, (x._1, g1(x._1))))
      val paramDF: DataFrame = convertLocalModelToDF(sqlContext, param, B_i_model_hash)
      paramDF.registerTempTable("Model")
    } else {
      throw new RuntimeException("Unsupported method:" + method)
    }
    Statistics.prepareParameters.addAndGet(System.nanoTime() - start)
  }

  def doSparkSQLJoinNCoGroup(sqlContext: SQLContext, method: String,
                             applyHash: Boolean,
                             g: Model2 => Data2 => Iterable[Delta2],
                             g1: Model2 => Object, g2: (Object, Data2) => Iterable[Delta2], numParts: Int): RDD[Output2] = {
    val start = System.nanoTime()

    val sqlStr: String =
      if (applyHash)
        """
                      SELECT d.id, m.model, d.data
                      FROM Model m, Data d
                      WHERE m.hash = d.hash
                      """
      else
        """
                      SELECT d.id, m.model, d.data
                      FROM Model m, Data d
                      WHERE B_i(m.model, d.data)
                      """
    var ret: RDD[Output2] = null
    if (method.compareToIgnoreCase("naive") == 0) {
      if (g == null) {
        throw new RuntimeException("The function g cannot be null");
      }

      val df = sqlContext.sql(sqlStr)
      System.out.println("SQL Plan for :" + method)
      df.explain(true)

      val temp: RDD[(Data2, (Long, Model2))] =
        df.rdd
          .map(r => (deserialize(r.get(2).asInstanceOf[Array[Byte]]).asInstanceOf[Data2],
            (r.get(0).asInstanceOf[Long],
              deserialize(r.get(1).asInstanceOf[Array[Byte]]).asInstanceOf[Model2])))
      //         .partitionBy(new HashPartitioner(numParts))
      //         .cache     
      //       
      //        temp.count

      // Step 3: Cogroup
      // Step 4: UDF invocation and final output assembly
      // TODO: Jacob: Please double check final output assembly
      ret = performAggregation(temp.map(x =>
        {
          val id: Long = x._2._1
          val d: Data2 = x._1
          val model: Model2 = x._2._2
          val itDelta: Iterable[Delta2] = g(model)(d)
          ((id, d), itDelta)
        }))
      //        ret = temp.map(x => ((x._1, x._3), x._2))
      //                         .groupByKey()
      //                         .flatMap(x => {
      //                           val d:Data2 = x._1._2
      //                           val id:Long = x._1._1
      //                           val models:Iterable[Model2] = x._2
      //                           var start = System.nanoTime()
      //                           val ret1 = models.map(g(_)(d)).map(agg(_, d)).map((id, _))
      //                           Statistics.groupByKeyFlatMapApplication.addAndGet(System.nanoTime() - start)                     
      //                           ret1
      //                           }  
      //                         ).values //.sortByKey().values
    } else if (method.compareToIgnoreCase("simulated-local") == 0) {
      // Join
      val temp: RDD[(Long, (Model2, Data2 => Iterable[Delta2]), Data2)] = sqlContext.sql(sqlStr).rdd
        .map(r => (r.get(0).asInstanceOf[Long],
          deserialize(r.get(1).asInstanceOf[Array[Byte]]).asInstanceOf[(Model2, Data2 => Iterable[Delta2])],
          deserialize(r.get(2).asInstanceOf[Array[Byte]]).asInstanceOf[Data2]))

      // Step 3: Cogroup
      // Step 4: UDF invocation and final output assembly
      // TODO: Jacob: Please double check final output assembly
      ret = performAggregation(temp.map(x => {
        val id: Long = x._1
        val d: Data2 = x._3
        val model: Model2 = x._2._1
        val appliedFn: (Data2 => Iterable[Delta2]) = x._2._2
        val itDelta: Iterable[Delta2] = appliedFn(d)
        ((id, d), itDelta)
      }))
      //        ret = temp.map(x => ((x._1, x._3), x._2))
      //                         .groupByKey()
      //                         .flatMap(x => {
      //                           val d:Data2 = x._1._2
      //                           val id:Long = x._1._1
      //                           val models:Iterable[(Model2, Data2 => Iterable[Delta2])] = x._2
      //                           var start = System.nanoTime()
      //                           val ret1 = models.map(_._2(d)).map(agg(_, d)).map((id, _))
      //                           Statistics.groupByKeyFlatMapApplication.addAndGet(System.nanoTime() - start)
      //                           ret1
      //                         }).values //.sortByKey().values
    } else if (method.compareToIgnoreCase("local") == 0 || method.compareToIgnoreCase("global") == 0) {

      if (g2 == null) {
        throw new RuntimeException("The function g2 cannot be null");
      }
      // Join 
      val temp: RDD[(Data2, (Long, (Model2, Object)))] = sqlContext.sql(sqlStr).rdd
        .map(r =>
          (deserialize(r.get(2).asInstanceOf[Array[Byte]]).asInstanceOf[Data2],
            (r.get(0).asInstanceOf[Long],
              deserialize(r.get(1).asInstanceOf[Array[Byte]]).asInstanceOf[(Model2, Object)])))
      //                      .partitionBy(new HashPartitioner(numParts))
      //                      .cache
      //        temp.count

      // Step 3: Cogroup
      // Step 4: UDF invocation and final output assembly
      // TODO: Jacob: Please double check final output assembly
      ret = performAggregation(temp.map(x =>
        {
          val id: Long = x._2._1
          val model: Model2 = x._2._2._1
          val appliedModel: Object = x._2._2._2
          val d: Data2 = x._1
          val itDelta: Iterable[Delta2] = g2(appliedModel, d)
          ((id, d), itDelta)
        }))
      //        ret = temp.map(x => ((x._1, x._3), x._2))
      //                         .groupByKey()
      //                         .flatMap(x => {
      //                           val d:Data2 = x._1._2
      //                           val id:Long = x._1._1
      //                           val models:Iterable[(Model2, Object)] = x._2
      //                           var start = System.nanoTime()
      //                           val ret1 = models.map(y => g2(y._2, d)).map(agg(_, d)).map((id, _))
      //                           Statistics.groupByKeyFlatMapApplication.addAndGet(System.nanoTime() - start)
      //                           ret1
      //                         }).values //.sortByKey().values
    } else {
      throw new RuntimeException("Unsupported method:" + method)
    }
    Statistics.doSparkSQLJoinNCoGroup.addAndGet(System.nanoTime() - start)
    ret
  }

}