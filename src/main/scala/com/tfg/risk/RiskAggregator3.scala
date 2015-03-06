package com.tfg.risk

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import java.text.SimpleDateFormat
import java.util.Date


object RiskAggregator3 {

case class Trades(tradeId: Long, asofDate: Date, attributes: Array[String])
  case class Leaves(nodeId: Int, book: String)
  case class PnlVector(tradeId: Long, asofDate: Date, pnlDays: Int, value: Double)
  //case class Output(nodeId: Int, asofDate: Date, value: Double)
  

  def main(args: Array[String]) {
    
    val startTime = System.currentTimeMillis();  
   

    val conf = new SparkConf().setAppName("Risk Aggregator").setMaster("local").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)

    val leaves = sc.textFile("/Users/dilip/tfg/tfg_poc/data/leaves.csv").map(_.split(",")).map(
      f => (f(1), Leaves(f(0).toInt, f(1))))

    val trades = sc.textFile("/Users/dilip/tfg/tfg_poc/data/trades.csv").map(_.split(",")).map(
      t => (t(2), Trades(t(1).toInt, new SimpleDateFormat("MM/dd/yy").parse(t(0)), t.slice(2, t.length - 1))))

    val pnlVectors = sc.textFile("/Users/dilip/tfg/tfg_poc/data/vectors_new.csv").map(_.split(",")).map(v => PnlVector(v(1).toLong, new SimpleDateFormat("MM/dd/yy").parse(v(0)),v(2).toInt, v(3).toDouble)).cache

    val t = pnlVectors.groupBy(f => f.tradeId).toArray()
    
    val tArray = pnlVectors.toArray()
    
    val (key,value) = t.head
    val daysArray = value.map(f => f.pnlDays).sliding(365).toArray.map(f => (f.head, f.last))
    val asOfDate = value.head.asofDate
    
    def inverted(key: Int, list: List[Int]): Map[Int, Int] = {

      list.map(x => x -> key).toMap
    }

    def joinValues(list: Iterable[Array[Long]]): String = {

      list.map(x => x.mkString(",")).mkString(",")
    }

    def filtered(tradeId: Int, item: (Int,Int)) = {
      
      println("Getting trade :" + tradeId + " for start date:" + item._1 +" and end date:" + item._2)
      
      tArray.filter(f => f.tradeId == tradeId).filter(f => f.pnlDays >= item._1).filter(f => f.pnlDays <= item._2)
    }

    def sumValues(arr: Array[PnlVector]) = {

      arr.map(_.value).sum
    }

    
    def calculate(input: (Int, String), item : (Int,Int)) = {
      
      val tradeIds = input._2.split(",")
      
       val millis = asOfDate.getTime();
       val startDate = new Date(millis - (item._1*24*60*60*1000) );
         
       val trades = tradeIds.map(f => filtered(f.toInt,item)).flatMap(f => f)
       
       

      val groupAndSum = trades.groupBy(f => f.pnlDays).map(f => (f._1 -> sumValues(f._2)))

      val sorted = groupAndSum.toList sortBy { _._2 }

      if (sorted.size > 0)
        (input._1, startDate ,sorted.last._2)
      else
        (input._1,startDate ,0.0)
      
    }
    
    
    
    def calculateVAR(input: (Int, String)) = {
     
      var i = 0
      val r = new Array[(Int,Date,Double)](daysArray.length)
      for(item <- daysArray) {
        
        r(i) = calculate(input,item)
        
        i = i+1
      }

     r

    }

    //    val hierarchy = sc.textFile("/Users/dilip/tfg/tfg_poc/data/hierarchy.csv").map(_.split(",")).map(f => f(1) -> f.slice(2, f.length))
    //    //.map(f => (f._1 -> f._2.mkString(",")))
    //    
    //    
    //   hierarchy.filter(f => f._2.size > 0).foreach(println)
    //   

    val hierarchy = sc.parallelize(Map(1 -> List(4, 5, 6, 7), 2 -> List(4, 5), 3 -> List(6, 7)).toSeq)

    val result = leaves.join(trades)

    val tradesNodes = result.map(f => f._2._1.nodeId -> f._2._2.tradeId).groupByKey().map(f => (f._1 -> f._2.toArray))

    
    
    val v1 = hierarchy.flatMap(f => inverted(f._1, f._2))

    val v2 = v1.join(tradesNodes).map(f => f._2._1 -> f._2._2).groupByKey.map(f => (f._1 -> joinValues(f._2))).collectAsMap()

    val v3 = v2.map(calculateVAR(_)).toSeq

    val v5 = tradesNodes.collectAsMap().map(f => f._1 -> f._2.mkString(",")).map(calculateVAR(_)).toSeq
    
    val v6 = v3.union(v5)
    val v4 = sc.parallelize(v6)

    v4.flatMap(f => f).saveAsTextFile("/Users/dilip/tfg/tfg_poc/data/output/" + System.currentTimeMillis()/1000 + "/")
    
    val endTime = System.currentTimeMillis();

    val duration = (endTime - startTime);  



     System.out.println("that took: " + duration/1000); 

  }

}