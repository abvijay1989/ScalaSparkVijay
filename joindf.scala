package org.salesintel.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class joindf {
 def injoindf (df1: DataFrame, df2: DataFrame, df1Key: String, df2Key: String): DataFrame = {
  df1.join(df2, df1(df1Key)===df2(df2Key),"inner") }
}