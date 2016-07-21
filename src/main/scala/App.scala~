import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SimpleApp {
  def main(args: Array[String]) {

val sc = new SparkContext("local[*]", "Intro (1)")
val data=sc.textFile("hdfs://localhost:/work/log.txt")
val splitRdd = data.map( line => line.split("\t") )
val pos = data.filter(_.split("\t")(0) == "1")
val neg = data.filter(_.split("\t")(0) == "0")
val tpos=pos.map(l=>l.split("\t")(1))
val tneg=pos.map(l=>l.split("\t")(1))
val listpos=tpos.map(l=>l.split(" "))
listpos.collect.foreach(l=>l.collect.foreach(println))
//System.out.println(w)
//System.out.println(pos.count())
//System.out.println(neg.count())

  }
}
