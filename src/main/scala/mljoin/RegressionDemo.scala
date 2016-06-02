package mljoin

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._


class RegressionDemo extends Serializable {
  def computeGrad(response:Double, dimID:Int, value:Double, regCoef: Double) {
    
  }
  
  def naiveRegression(sqlContext:SQLContext, dataRDD:RDD[(Int, Double)], featureRDD:RDD[(Int, Int, Double)], coefsRDD:RDD[(Int, Double)]) {
    import sqlContext.implicits._
    val data = dataRDD.toDF("dataID", "response")
    val features = featureRDD.toDF("dataID", "dimID", "value")
    val coefs = coefsRDD.toDF("dimID", "regCoef")
    
    data.registerTempTable("data")
    features.registerTempTable("features")
    coefs.registerTempTable("coefs")
    
    val produced = sqlContext.sql(
        """ SELECT computeGrad(x.response, f.dimID, f.value, c.regCoef)
            FROM features f, coefs c, data x
            WHERE f.dataID = x.dataID AND
            f.dimID = c.dimID
            """)
  }
}