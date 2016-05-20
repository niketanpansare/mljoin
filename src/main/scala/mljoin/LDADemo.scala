package mljoin

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._

class LDADemo extends Serializable {
  
  def assignTopicsNaive(topicID:Int, wordProbs:Vector, topicProbs:Vector, docID:Int, wordCnts:Vector) = {
    // TODO:  
    wordProbs
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
        """ SELECT assignTopicsNaive(t.topicID, t.wordProbs, tp.topicProbs, d.docID, d.wordCnts)
            FROM topics t, topicProbs tp, docs d
            WHERE tp.docID = d.docID
            """)
    
    // TODO:
    // val tmp = produced.rdd.flatMap( ... ).reduceByKey( ... )
    produced.show
    
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