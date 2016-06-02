package mljoin

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._

class LDADemo extends Serializable {
  
  val numTopics: Int = 100
  val vocabSize: Int = 100
  
  // Iterable[(topicID:Int, wordProbs:Vector, topicProbs:Vector, wordCnts:Vector)]
  def assignTopicsNaive(docID:Int, other:Iterable[(Int, Vector, Vector, Vector)]) = {
    // TODO:  
    var wordCnts11:DenseMatrix = DenseMatrix.zeros(numTopics, vocabSize) 
    for(vals <- other) {
      val topicID:Int = vals._1
      val wordProbs:Vector = vals._2
      val topicProbs:Vector = vals._3
      val wordCnts:Vector = vals._4
      // TODO: Jacob
      // ret = ret + wordProbs.numNonzeros
    }
    wordCnts11
  }
  
  def naiveLDA(sqlContext:SQLContext, topicsRDD:RDD[(Int, Vector)], topicProbsRDD:RDD[(Int, Vector)], docsRDD:RDD[(Int, Vector)]): 
      (RDD[(Int, Vector)], RDD[(Int, Vector)]) = {
    
    import sqlContext.implicits._
    val topics = topicsRDD.toDF("topicID", "wordProbs")
    val topicProbs = topicProbsRDD.toDF("docID", "topicProbs")
    val docs = docsRDD.toDF("docID", "wordCnts")
    
    topics.registerTempTable("topics")
    topicProbs.registerTempTable("topicProbs")
    docs.registerTempTable("docs")
    sqlContext.udf.register("assignTopicsNaive", assignTopicsNaive _)
    val produced = sqlContext.sql(
        """ SELECT d.docID, t.topicID, t.wordProbs, tp.topicProbs, d.wordCnts
            FROM topics t, topicProbs tp, docs d
            WHERE tp.docID = d.docID
            """).rdd.map(x => (x(0), (x(1), x(2), x(3), x(4)))).groupByKey.map(x => assignTopicsNaive(x._1.toString.toInt, x._2.asInstanceOf[Iterable[(Int, Vector, Vector, Vector)]]))
    
    // TODO:
    // val tmp = produced.rdd.flatMap( ... ).reduceByKey( ... )
    // produced.show
    
    (topicsRDD, topicProbsRDD)
  }
  
  def loopNaiveLDA(sqlContext:SQLContext, initialTopicsRDD:RDD[(Int, Vector)], initialTopicProbsRDD:RDD[(Int, Vector)], 
      docsRDD:RDD[(Int, Vector)], numIter:Int) {
    var topicsRDD = initialTopicsRDD
    var topicProbsRDD = initialTopicProbsRDD
    for(i <- 1 to numIter) {
      val tmp = naiveLDA(sqlContext, topicsRDD, topicProbsRDD, docsRDD)
      topicsRDD = tmp._1
      topicProbsRDD = tmp._2
    }
  }
  
  def randData(sc:SparkContext):RDD[(Int, Vector)] = {
    sc.parallelize(1 to 100,5).map( x => (x, 
        new DenseVector(
        breeze.linalg.DenseVector.rand(10, breeze.stats.distributions.Gaussian(0, 1)).toArray )) )
  }
  
}