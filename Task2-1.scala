

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DataLoader {

    val booksRating = spark.read.csv("./Books_rating.csv") //loads books_rating as a DataFrame
    // books_rating.csv: 
    //10 columns: id, title, price, user_id, profile_name, review/helpfulness, review/score, 
    //review/time, review/summary, review/text

    val booksData = spark.read.csv("./books_data.csv") // loads books_data as a DataFrame
    // books_data.csv:
    //10 columns: title, description, authors, image, previewLink, publisher, publishedDate,
    //infoLink, categories, ratingsCount
}
