package com.tfg.risk

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object RiskAggregator2 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Risk Aggregator 2").setMaster("local")
    val sc = new SparkContext(conf)

    val pnlvectors = sc.textFile("/Users/dilip/tfg/tfg_poc/data/vectors1.csv").map(_.split(","))

    val grouped = pnlvectors.groupBy { case Array(date1, id, pnldate, v) => pnldate }

    // this will give a RDD with dates and trades 
    // date1 -> trade1,trade2,trade3 ...
    // date2 -> trade2,trade3,trade4 ... etc
    val tradesByDate = grouped.map(f => (f._1 , f._2.map(p => p(1)))).map(f => (f._1 -> f._2.mkString(",")))
    
     val valuesByDate = grouped.map(f => (f._1 , f._2.map(p => p(3).toDouble))).map(f => (f._1 -> f._2.reduceLeft(_ + _)))
    
     
    val sorted = valuesByDate.sortBy(_._2)
    
    val VAR = sorted.first()
    
    
  }

}