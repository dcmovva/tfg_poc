package tfg_poc

import java.io._
import java.util.{Date, Calendar, GregorianCalendar, Random}
import java.text.DateFormat
import java.text.SimpleDateFormat

object ScalaScript {

  def main(args: Array[String]) {
    //<date, trade id, pnl date, value>

    val trades = List ("trade1","trade2","trade3")
		var calendar = new GregorianCalendar(2012,8,20)
    val pnl = new GregorianCalendar(2014,12,20)
	  var sdf = new SimpleDateFormat("yyyy/MM/dd")
    //var date = Date
    //val trade
    
    val fw = new PrintWriter(new File("/Users/dilip/throwaway/tfs_poc/data/pnl.txt"))
    //val fw = new FileWriter("/Users/dilip/throwaway/tfs_poc/data/pnl.txt", true)
    
		trades.foreach(f => 
		for(i <- 1 to 250 )  {
		  val ran = new Random()
		  val code= (100000 + ran.nextInt(900000)).toString()
		  calendar.add(Calendar.DATE, 1)
        fw.write(sdf.format(calendar.getTime()) + "," + f + "," + sdf.format(pnl.getTime()) + "," + code + "\n\r")
		})
		fw.close
  }
}