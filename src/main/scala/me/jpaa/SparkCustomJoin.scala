package me.jpaa

import java.nio.file.Paths

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object SparkCustomJoin {
    def main(args: Array[String]) {

      implicit val spark = SparkSession.builder().appName("Custom Joins").master("local[8]").getOrCreate()
      val sqlContext = spark.sqlContext

      // load dataframes from files under /resources
      val huluDf = loadDF(fsPath("/hulu_movies.csv"))
      huluDf.createOrReplaceTempView("hulumovies")
      huluDf.show()

      val netflixDf = loadDF(fsPath("/netflix_movies.csv"))
      netflixDf.createOrReplaceTempView("netflixmovies")
      netflixDf.show()
      netflixDf.schema.printTreeString()

      //define our UDF
      def similar(desc1:String, desc2:String): Double ={ //returns a ratio
        val tags1 = desc1.split(" ").map(_.trim).toSet
        val tags2 = desc2.split(" ").map(_.trim).toSet

        //now tags1 and tags2 are each a set of Strings, each being a tag
        tags1.intersect(tags2).size.toDouble / (tags1.size + tags2.size)
      }

      sqlContext.udf.register("similar", similar(_:String, _:String))

      val sql = s"""SELECT netflixmovies.title, hulumovies.title, SIMILAR(netflixmovies.tags, hulumovies.tags) as score
                   |FROM netflixmovies
                   |JOIN hulumovies ON SIMILAR(netflixmovies.tags, hulumovies.tags) > 0.2""".stripMargin

      // select similar movies according to criteria
      val similarMovies = sqlContext.sql(sql
      )

      // evaluate the above SQL and show results
      similarMovies.show()
    }

    /** @return The filesystem path of the given resource */
    def fsPath(resource: String): String =
      Paths.get(getClass.getResource(resource).toURI).toString

    def loadDF(path:String)(implicit spark: SparkSession) : DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(path)
}
