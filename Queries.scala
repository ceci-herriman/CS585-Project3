import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//prereqs: need to install sbt on computer and have the proper file structure in your folder where your .scala file is
// I also have the scala metals vsc extension

//to compile: run sbt package
//to run in docker container: run ~/spark/bin/spark-submit --class Query1 ./target/scala-2.12/cs585-project3_2.12-0.1.0.jar
  //this is run in my shared_folder/project3/ directory, where my scala file + spark sbt file structure is
  //so when I make new data with data.py or recompile my .scala, I don't have to copy files over

object Query1 {

  def isClose(x1: Double, y1: Double, x2: Double, y2: Double): Boolean = {
    val dx = x1 - x2
    val dy = y1 - y2
    (dx * dx + dy * dy) <= 36.0
  }

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Query1")
    val sc = new SparkContext(sparConf)
    val p = sc.textFile("file:////home/ds503/shared_folder/project3/People.txt")
    val c = sc.textFile("file:////home/ds503/shared_folder/project3/Connected.txt")

    //convert to key value pairs for easy joining
    val people = p.map(line => {
      val parts = line.split(",")
      (parts(0), (parts(1).toDouble, parts(2).toDouble, parts(3), parts(4), parts(5)))
    })

    val connected = c.map(line => {
      val parts = line.split(",")
      (parts(0), (parts(1).toDouble, parts(2).toDouble, parts(3), parts(4), parts(5)))
    })

    //cache connected
    val connectedBroadcast = sc.broadcast(connected.collect())

    //for each person, get all matches to those in connected and return list of all matches
    //this is faster than cartesian then filtering because we filter as we match records together instead of building
    //cartesian product and then going through it again
    val res = people.flatMap { case (id2, (x2, y2, a2, b2, c2)) =>
      connectedBroadcast.value.collect {
        case (id1, (x1, y1, a1, b1, c2)) if id1 != id2 && isClose(x1, y1, x2, y2) =>
          ((id1, x1, y1, a1, b1, c2), (id2, x2, y2, a2, b2, c2))
      }
    }

    res.foreach(println)

    sc.stop()
  }
}

