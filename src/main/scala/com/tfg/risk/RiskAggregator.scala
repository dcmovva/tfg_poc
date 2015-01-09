package com.tfg.risk

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import java.text.SimpleDateFormat
import java.util.Date

object RiskAggregator {

  case class Trades(tradeId: Long, asofDate: Date, attributes: Array[String])
  case class Leaves(nodeId: Int, book: String)
  case class PnlVector(tradeId: Long, asofDate: Date, pnlDate: Date, value: Double)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Risk Aggregator").setMaster("local")
    val sc = new SparkContext(conf)

    val leaves = sc.textFile("/Users/dilip/tfg/tfg_poc/data/leaves.csv").map(_.split(",")).map(
      f => (f(1), Leaves(f(0).toInt, f(1))))

    val trades = sc.textFile("/Users/dilip/tfg/tfg_poc/data/trades.csv").map(_.split(",")).map(
      t => (t(2), Trades(t(1).toInt, new SimpleDateFormat("MM/dd/yy").parse(t(0)), t.slice(2, t.length - 1))))

    val pnlVectors = sc.textFile("/Users/dilip/tfg/tfg_poc/data/vectors1.csv").map(_.split(",")).map(v => PnlVector(v(1).toLong, new SimpleDateFormat("MM/dd/yy").parse(v(0)), new SimpleDateFormat("MM/dd/yy").parse(v(2)), v(3).toDouble))

    def inverted(key: Int, list: List[Int]): Map[Int, Int] = {

      list.map(x => x -> key).toMap
    }

    def joinValues(list: Iterable[Array[Long]]): String = {

      list.map(x => x.mkString(",")).mkString(",")
    }

    def filtered(tradeId: Int) = {
      pnlVectors.filter(f => f.tradeId == tradeId).toArray()
    }

    def sumValues(arr: Array[PnlVector]) = {

      arr.map(_.value).sum
    }

    def calculateVAR(input: (Int, String)) = {

      val tradeIds = input._2.split(",")

      val trades = tradeIds.map(f => filtered(f.toInt)).flatMap(f => f)

      val groupAndSum = trades.groupBy(f => f.pnlDate).map(f => (f._1 -> sumValues(f._2)))

      val sorted = groupAndSum.toList sortBy { _._2 }

      if (sorted.size > 0)
        (input._1, sorted.last._2)
      else
        (input._1, 0.0)

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

    v4.saveAsTextFile("/Users/dilip/tfg/tfg_poc/data/output")

  }

}