import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//ssh -p 14226 ds503@localhost

//prereqs: need to install sbt on computer and have the proper file structure in your folder where your .scala file is
// I also have the scala metals vsc extension

//my build.sbt file:
// ThisBuild / scalaVersion := "2.12.18"
// ThisBuild / version      := "0.1.0"

// lazy val root = (project in file("."))
//   .settings(
//     name := "CS585-Project3",
//     libraryDependencies ++= Seq(
//       "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
//       "org.apache.spark" %% "spark-sql"  % "3.5.0" % "provided"
//     )
//   )

//to compile: run sbt package
//to run in docker container: run ~/spark/bin/spark-submit --class Query1 ./target/scala-2.12/cs585-project3_2.12-0.1.0.jar
  //this is run in my shared_folder/project3/ directory, where my scala file + spark sbt file structure is
  //so when I make new data with data.py or recompile my .scala, I don't have to copy files over

object Query3 {
  def isClose(x1: Double, y1: Double, x2: Double, y2: Double): Boolean = {
    val dx = x1 - x2
    val dy = y1 - y2
    (dx * dx + dy * dy) <= 36.0
  }

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Query1")
    val sc = new SparkContext(sparConf)
    val p = sc.textFile("file:////home/ds503/shared_folder/project3/People_with_handshake_info.txt")

    //map --> (id, (x, y)) if connected is yes --> connected and cache it since its assumed to be small in project desc
    val connected = p
      .map(_.split(","))
      .filter(parts => parts(6) == "yes")
      .map(parts => (parts(0), (parts(1).toDouble, parts(2).toDouble)))


    val connectedBroadcast = sc.broadcast(connected.collect())

    //for each person, map (id, 1) for each connected person with id "id" they are close to
    //case (id1, (x1, y1, a1, b1, c1))
    val res = p
    .flatMap { line =>
      val parts = line.split(",")
      connectedBroadcast.value.collect {
        //get the connected ids the person is close to and map them out
        case (id2, (x2, y2)) 
        if parts(0) != id2 && isClose(parts(1).toDouble, parts(2).toDouble, x2, y2) => id2
      }
    }
    .map(id2 => (id2, 1))
    .reduceByKey(_ + _) //add up all values
    .collect()

    res.foreach(println)

    sc.stop()
  }
}

//I need to optimized these following queries more later - ceci

object Query2 {
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

    //return pi, connect-i for those which are close to each toher
    val res = people.flatMap { case (id2, (x2, y2, a2, b2, c2)) =>
      connectedBroadcast.value.collect {
        case (id1, (x1, y1, a1, b1, c1)) if id1 != id2 && isClose(x1, y1, x2, y2) => (id2)
      }
    }.distinct()

     .flatMap { line =>
      val parts = line.split(",")
      connectedBroadcast.value.collect {
        //get the connected ids the person is close to and map them out
        case (id2, (x2, y2)) 
        if parts(0) != id2 && isClose(parts(1).toDouble, parts(2).toDouble, x2, y2) => id2
      }
    }

    res.foreach(println)

    sc.stop()
  }
}


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

    //return pi, connect-i for those which are close to each toher
    val res = people.flatMap { case (id2, (x2, y2, a2, b2, c2)) =>
      connectedBroadcast.value.collect {
        case (id1, (x1, y1, a1, b1, c1)) if id1 != id2 && isClose(x1, y1, x2, y2) =>
          ((id2, x2, y2, a2, b2, c2), (id1, x1, y1, a1, b1, c1))
      }
    }

    res.foreach(println)

    sc.stop()
  }
}

