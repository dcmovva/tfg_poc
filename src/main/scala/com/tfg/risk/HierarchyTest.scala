package com.tfg.risk

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import java.text.SimpleDateFormat
import java.util.Date

object HierarchyTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Risk Aggregator").setMaster("local")
    val sc = new SparkContext(conf)

    val hierarchy = sc.textFile("/Users/dilip/tfg/tfg_poc/data/hierarchy.csv").map(_.split(",")).map(f => f(1) -> f.slice(2, f.length))

    val v = hierarchy.map(f => (f._1 , f._2.mkString(",")))
    
    hierarchy.foreach(println)

    //
    //.groupBy(f => f._1).flatMap(f => f._2).map(f => f._1 -> f._2.mkString(","))
    
   //val r =  hierarchy.filter(f => f._2.size > 0).map(f => f._2.map(i => f._1 ->  i.filter(_ == f._1)))
  }
}