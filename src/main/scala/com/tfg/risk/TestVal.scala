package com.tfg.risk
import scala.io.Source

object TestVal {

  
  def explode(str : String) : Array[String] = {
    
    val tokens = str.split(",")
    val subset = tokens.drop(2)
    
    val lists = subset.partition(_.contains("<"))
    
    val joined = lists._1.zip(lists._2)
    
    
    joined.map(f => "%s,%s,%s,%s".format(tokens(0), tokens(1), f._1, f._2))
    
  }
  def main(args: Array[String]) {
    val filename = "/Users/dilip/throwaway/tfs_poc/data/trade.txt"
    for (line <- Source.fromFile(filename).getLines()) {
      explode(line).foreach(println)
    }
  }

}