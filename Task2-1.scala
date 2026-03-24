

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.*

object DataLoader {

    def main(){

        spark.sql("drop table if exists " + books_rating)

        val booksRating = spark.read.csv("./Books_rating.csv").write.saveAsTable("books_rating") //loads books_rating as a DataFrame
        // books_rating.csv: 
        //10 columns: id, title, price, user_id, profile_name, review/helpfulness, review/score, 
        //review/time, review/summary, review/text

        spark.sql("drop table if exists " + books_data)
        val booksData = spark.read.csv("./books_data.csv").write.saveAsTable("books_data") // loads books_data as a DataFrame
        // books_data.csv:
        //10 columns: title, description, authors, image, previewLink, publisher, publishedDate,
        //infoLink, categories, ratingsCount

        spark.read.table("books_data").show()
    }
}
