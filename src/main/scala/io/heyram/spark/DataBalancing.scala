package io.heyram.spark

import io.heyram.spark.jobs.FraudDetectionTraining._
import org.apache.log4j.Logger
import org.apache.spark.ml.clustering.{KMeansModel, KMeans}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


object DataBalancing {

  val logger = Logger.getLogger(getClass.getName)
  /*
  There will be more normal transaction then anomaly transaction. So non-fraud transactions must be balanced
  Kmeans Algorithm is used to balance normal transatiion.
  No. of normal transactions  are balanced(reduced) to no. of fraud transaction
   */
  def createBalancedDataframe(df:DataFrame, reductionCount:Int)(implicit sparkSession:SparkSession) = {

    val kMeans = new KMeans().setK(reductionCount).setMaxIter(30)
    val kMeansModel = kMeans.fit(df)

    import sparkSession.implicits._
    kMeansModel.clusterCenters.toList.map(v => (v, 0)).toDF("features", "label")
  }
}
