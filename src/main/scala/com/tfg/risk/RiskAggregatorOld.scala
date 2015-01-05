package com.tfg.risk
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Random
object RiskAggregatorOld {

  
  case class Trades(tradeId: Long,asofDate: Date, attributes: Array[String])
  case class Leaves(nodeId: Int,book: String)
  case class PnleVectors(tradeId: Long,asofDate: Date, pnlData: Array[(Seq[(String,Double)])])
  case class PnlData(tradeId: Long,asofDate: Date, varDate: Date, VAR : Double, pnlData: Seq[(String,Double)])
  
  
  def main(args: Array[String]) {
    
  val nodes = List(4,5,6,7)
    
  val conf = new SparkConf().setAppName("Risk Aggregator").setMaster("local")
  val sc = new SparkContext(conf)

  
  val leaves = sc.textFile("/Users/dilip/tfg/tfg_poc/data/leaves.csv").map(_.split(",")).map(
     f => (f(1), Leaves(f(0).toInt, f(1)))
  )

  
  val trades = sc.textFile("/Users/dilip/tfg/tfg_poc/data/trades.csv").map(_.split(",")).map(
     t => (t(2),Trades(t(1).toInt, new SimpleDateFormat("MM/dd/yy").parse(t(0)),t.slice(2, t.length-1)))
  )

  
  val result =  leaves.join(trades)
 
  val m =  result.map(f => f._2._1.nodeId -> f._2._2.tradeId).groupByKey()
  
 
    
    m.foreach(println)
   //val lines = Source.fromFile("").getLines.map(l => l.split(","))
  
  
  def calculateVar(tradeId : Int, asofDate : Date, pnlVectors : Seq[(String,Double)]) = {
      val sorted = pnlVectors.sortBy(_._2)
      val VAR = sorted((0.01*365).toInt)
      
      PnlData(tradeId,asofDate,new SimpleDateFormat("MM/dd/yy").parse(VAR._1),VAR._2, pnlVectors)
    }
    
//    def zipValues(tradeId : Long,asofDate : Date,datesArray : Array[Seq[String]],pnlsArray : Array[Seq[String]]) : PnlVectors = {
//      
//      PnlVectors(tradeId,asofDate,datesArray.zip(pnlsArray))
//    }
    
    def zipValues(rdd : ((String,String) , Iterable[Array[String]])) : Array[PnlData] = {
        val tradeId = rdd._1._2.toInt
        val asofDate = new SimpleDateFormat("MM/dd/yy").parse(rdd._1._1)
        val datesArray = rdd._2.map(v => v(2)).toSeq.sliding(365).toArray
        val pnlsArray = rdd._2.map(v => v(3).toDouble).toSeq.sliding(365).toArray
        val zipped = datesArray.zip(pnlsArray)
        val r =  zipped.map( f => f._1.zip(f._2))
        
      r.map(i => calculateVar(tradeId,asofDate,i))
    }
    
    
    val pnlvectors = sc.textFile("/Users/dilip/tfg/tfg_poc/data/vectors1.csv").map(_.split(","))
    
   val grouped = pnlvectors.groupBy{case Array(date1,id,pnldate, v) => (date1,id)}
    //val t = pnlvectors.groupBy(_(1))//.map(pair => (pair._1, pair._2.forall(p)(2)))
    
   // println(grouped.count())
    grouped.foreach(f => println(f._1))    
    
      val r = grouped.map(f => zipValues(f))
      
      
   //   r.foreach(x => println(x.))
    //val tradesVector = lines.map(v => Trades(new SimpleDateFormat("MM/dd/yy").parse(v(0)),v(1).toLong,new SimpleDateFormat("MM/dd/yy").parse(v(2)),v(3).toDouble))
    
//     val tradeId = 1000L
//     val asofDate = new SimpleDateFormat("MM/dd/yy").parse("11/21/14")
//     val datesArray = lines.map(v => v(2)).sliding(365).toArray
//     val pnlsArray = lines.map(v => v(3)).sliding(365).toArray
//     
//     val ret = zipValues(tradeId, asofDate, datesArray,pnlsArray)
//     
//     val VaR = calculateVar(ret)
   
  }
}