package org.salesintel.spark

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
                           
object signalReport {
  def main(args:Array[String]):Unit={
    
    
  val spark=SparkSession.builder().appName("Sample sql app").master("local[*]")
            .getOrCreate();

    val sc=spark.sparkContext
    sc.setLogLevel("error")

    val sqlc= spark.sqlContext
    
    import sqlc.implicits._
    
    if (args.length == 0) {
      println("Input File Path is missing.. Program aborted.")
      spark.stop()
    }
    
    /* val location = "home/hduser/hive/data" */
    
    val location = args(0)
    
    val windowedPartition = Window.partitionBy("entity_id")

    val signal   = spark.read.parquet("file:////"+ location + "/signals/*")
    
    val signal1  = (signal.withColumn("max_month_id",max($"month_id").over(windowedPartition))
                          .withColumn("min_month_id",min($"month_id").over(windowedPartition)))
                        
    val signal2  = (signal1.where($"month_id" === $"max_month_id")
                          .groupBy($"entity_id").agg(min($"item_id").as("newest_item_id")))
    
    val signal3  = (signal1.where($"month_id" === $"min_month_id")
                           .groupBy($"entity_id").agg(min($"item_id").as("oldest_item_id"))
                           .withColumnRenamed("entity_id","entity_id1"))
                         
    val signal4  = (signal1.groupBy($"entity_id")
                           .agg(sum($"signal_count").as("total_signals"))
                           .withColumnRenamed("entity_id","entity_id2"))
                         
  /*  val signal5  = (signal2.join(signal3, signal2("entity_id")===signal3("entity_id1"),"inner")
                           .select("entity_id","oldest_item_id","newest_item_id")) */
    val obj_joindf = new joindf
      
    val signal5  = (obj_joindf.injoindf(signal2,signal3,"entity_id","entity_id1")
                             .select("entity_id","oldest_item_id","newest_item_id"))
                           
  /*  val finaldf   = (signal5.join(signal4, signal5("entity_id")===signal4("entity_id2"),"inner")
                           .select("entity_id","oldest_item_id","newest_item_id","total_signals")) */
                           
    val finaldf  = (obj_joindf.injoindf(signal5,signal4,"entity_id","entity_id2")
                             .select("entity_id","oldest_item_id","newest_item_id","total_signals"))
                             
    finaldf.coalesce(1).write.mode("overwrite").parquet("file:////"+ location + "/Result")
    
    finaldf.show()
    println("Completed Successfully")

  }
}

