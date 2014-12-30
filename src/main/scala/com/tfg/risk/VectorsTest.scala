package com.tfg.risk

import java.util.Date
import java.text.SimpleDateFormat
import scala.io.Source

object VectorsTest {
  
  case class Trades(tradeId: Long,asofDate: Date, pnlData: Array[(Seq[String],Seq[String])])
  
  def main(args: Array[String]) {
  
    
    println("printing")
    def calculateVar(trades : Trades) : Double = {
      
      131221.23
    }
    
    def zipValues(tradeId : Long,asofDate : Date,datesArray : Array[Seq[String]],pnlsArray : Array[Seq[String]]) : Trades = {
      
      Trades(tradeId,asofDate,datesArray.zip(pnlsArray))
    }
    
    val filename = "/Users/dilip/throwaway/tfs_poc/data/vectors1.csv"
    
    val lines = Source.fromFile(filename).getLines.map(l => l.split(","))
    
    val t = lines.toList.groupBy(_(1))
    
    t.foreach(println)
    
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