package com.tfg.risk

import scala.io.Source


object TestVectors {

   case class PnlData(asofDate: String, tradeId : Int, days : Int,pnlData: Double)
   
  def main(args: Array[String]) {
     val filename = "/Users/dilip/tfg/tfg_poc/data/vectors_new.csv"
    
    val lines = Source.fromFile(filename).getLines.map(l => l.split(",")).map(v => PnlData(v(0), v(1).toInt, v(2).toInt , v(3).toDouble))

    
    val t = lines.toList.groupBy(f => f.tradeId).toMap
    val (key,value) = t.head
    val daysArray = value.map(f => f.days).sliding(365).toArray
    
    val v =  daysArray.map(f => (f.head, f.last))
    
    println(v.size)
    
    //println(key + "," + daysArray.mkString(","))
    
  }
}