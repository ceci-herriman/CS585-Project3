import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
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

object Query4 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Part2").master("local[*]").getOrCreate()

    val reviews = spark.read
      .option("header", "true")     //fist line of csv is header
      .option("inferSchema", "true")
      .option("nullValue", "")
      .csv("file:////home/ds503/shared_folder/project3/Books_rating.csv")

    //filter reviews 
    //val T1 = reviews.filter(reviews("review/score") > 3.0 
    //                  && reviews("review/text").isNotNull && reviews("Title").isNotNull
     //                 && reviews("review/text") != "" && reviews("Title") != "")


    // T1.write
    //   .option("header", "true")
    //   .mode("overwrite")
    //   .csv("file:////home/ds503/shared_folder/project3/T1_output.csv")

    //group reviews by review/score and compute summary statistics
    //number of reviews, average review text length, minimum review text length,  maximum review text length
    val groupedT1Count = reviews.groupBy(reviews("review/score")).count()
   // val groupedT1 = T1.groupBy(T1("review/score")).agg(avg(T1("review/text").count()))

    groupedT1Count.show()
    spark.stop()
  }
}

object Query3 {
  def isClose(x1: Double, y1: Double, x2: Double, y2: Double): Boolean = {
    val dx = x1 - x2
    val dy = y1 - y2
    (dx * dx + dy * dy) <= 36.0
  }

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Part1")
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


object Query2 {
  def isClose(x1: Double, y1: Double, x2: Double, y2: Double): Boolean = {
    val dx = x1 - x2
    val dy = y1 - y2
    (dx * dx + dy * dy) <= 36.0
  }

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Part1")
    val sc = new SparkContext(sparConf)
    val p = sc.textFile("file:////home/ds503/shared_folder/project3/People.txt")
    val c = sc.textFile("file:////home/ds503/shared_folder/project3/Connected.txt")

    val connected = c.map(line => {
      val parts = line.split(",")
      (parts(0), (parts(1).toDouble, parts(2).toDouble))
    })

    //cache connected
    val connectedBroadcast = sc.broadcast(connected.collect())

    //for each person, get all matches to those in connected and return list of all matches
    //this is faster than cartesian then filtering because we filter as we match records together instead of building
    //cartesian product and then going through it again

    //return pi for those which are close to each a connected person
    val res = p.flatMap { line =>
      val parts = line.split(",")
      connectedBroadcast.value.collect {
        //if person is close to connected person, return their id
        case (id2, (x2, y2)) 
        if parts(0) != id2 && isClose(parts(1).toDouble, parts(2).toDouble, x2, y2) => parts(0)
      }
    }
    .distinct()

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
    val sparConf = new SparkConf().setMaster("local").setAppName("Part1")
    val sc = new SparkContext(sparConf)
    val p = sc.textFile("file:////home/ds503/shared_folder/project3/People.txt")
    val c = sc.textFile("file:////home/ds503/shared_folder/project3/Connected.txt")

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
    val res = p.flatMap { line =>
      val parts = line.split(",")
      connectedBroadcast.value.collect {
        //if person is close to connected person, return them as a pair
        case (id1, (x1, y1, a1, b1, c1))
        if parts(0) != id1 && isClose(parts(1).toDouble, parts(2).toDouble, x1, y1) => 
          ((parts(0), parts(1).toDouble, parts(2).toDouble, parts(3), parts(4), parts(5)), (id1, x1, y1, a1, b1, c1))
      }
    }

    res.foreach(println)

    sc.stop()
  }
}

