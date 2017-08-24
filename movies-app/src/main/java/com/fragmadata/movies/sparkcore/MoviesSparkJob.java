package com.fragmadata.movies.sparkcore;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import com.fragmadata.movies.bean.Movie;
import com.fragmadata.movies.bean.Rating;
import com.fragmadata.movies.bean.User;
import com.fragmadata.movies.context.SparkContext;
import com.fragmadata.movies.util.XLSXUtils;

import scala.runtime.AbstractFunction1;

/**
 * @author Muthamizhan
 * 
 *         Core spark job to execute spark jobs. Handles loading data files and process them.
 *
 */
public class MoviesSparkJob implements Serializable {

  private static final long serialVersionUID = 1L;
  public static String USERS_FILE_PATH = "";
  public static String MOVIES_FILE_PATH = "";
  public static String RATINGS_FILE_PATH = "";
  private HashMap<String, String> occupationMap = new HashMap<>();

  /**
   * Constructor to call spark context to initialize spark context instance.
   * 
   * @param sparkMaster
   * @param appName
   * @throws Exception
   */
  public MoviesSparkJob(String sparkMaster, String appName) throws Exception {
    // call to initialize spark context
    SparkContext.initializeJavaSparkContext(sparkMaster, appName);
  }

  public void process() {
    // loading data from .dat
    Dataset<Row> userdf = loadUserData();
    Dataset<Row> moviesdf = loadMoviesdata();
    Dataset<Row> ratingsdf = loadRatingsdata();
    // Processing and generating reports
    generateTopMostViewedReport(moviesdf, ratingsdf);
    generateTopMostRatedReport(moviesdf, ratingsdf);
    generateGenreRankingReport(userdf, moviesdf, ratingsdf);
    // flush data into disk from memory.
    XLSXUtils.flush();
  }

  /**
   * Logic to generate top twenty rated movies. (the movie should be viewed / rated by at least 40
   * users)
   * 
   * @param moviesdf
   * @param ratingsdf
   */
  private void generateTopMostRatedReport(Dataset<Row> moviesdf, Dataset<Row> ratingsdf) {
    // Ordering all ratings record with respect to rating by
    // desc order then grouping them by movies which are watched at least by 40 users.
    // Then taking top 20 from the result set
    Dataset<Row> topRatedDataSet =
        ratingsdf.orderBy(org.apache.spark.sql.functions.col("ratings").desc()).groupBy("movieId")
            .count().where(org.apache.spark.sql.functions.col("count").$greater$eq(40))
            .orderBy(org.apache.spark.sql.functions.col("count").desc()).limit(20);
    List<String> movieNames = new ArrayList<>();
    // Collecting data set
    for (Row row : topRatedDataSet.collectAsList()) {
      String movieName = (String) moviesdf.select("movieName")
          .where(col("movieId").equalTo(row.get(0))).collectAsList().get(0).get(0);
      movieNames.add(movieName);
    }
    XLSXUtils.writeReport2Intoxls(movieNames);
  }

  /**
   * Logic to generate top ten most viewed movies with their movie names.
   * 
   * @param moviesdf
   * @param ratingsdf
   */
  private void generateTopMostViewedReport(Dataset<Row> moviesdf, Dataset<Row> ratingsdf) {
    // selecting most viewed movies by grouping them. And selecting top-10 from the data set.
    Dataset<Row> groupddf = ratingsdf.groupBy("movieId").count()
        .orderBy(org.apache.spark.sql.functions.col("count").desc()).limit(10);
    List<String> movieNames = new ArrayList<>();
    for (Row row : groupddf.collectAsList()) {
      String movieName =
          (String) moviesdf.select("movieName").where(col("movieId").equalTo(row.get(0)))
              .orderBy("movieName").collectAsList().get(0).get(0);
      movieNames.add(movieName);
    }
    XLSXUtils.writeReport1(movieNames);
  }

  /**
   * Functionalities to find Genres ranked by average rating for each age and profession group.
   * 
   * @param userdf - user data set.
   * @param moviesdf - movies data set
   * @param ratingsdf - ratings data set
   */
  private void generateGenreRankingReport(Dataset<Row> userdf, Dataset<Row> moviesdf,
      Dataset<Row> ratingsdf) {
    // categorizing user's age based on ranges
    Dataset<Row> filteredUserdf = userdf.filter(col("age").gt(17))
        .withColumn("age", when(col("age").$greater$eq(51), "50+").otherwise(col("age")))
        .withColumn("age", when(col("age").$greater$eq(36), "36-50").otherwise(col("age")))
        .withColumn("age", when(col("age").$greater$eq(18), "18-35").otherwise(col("age")));

    // Generating occupation with age combination by grouping them, Then collecting all user's
    // ids in order to look into pull data from ratings table
    Dataset<Row> groupddf = filteredUserdf.groupBy(col("occupation"), col("age"))
        .agg(collect_list(col("userId")).alias("userId"))
        .orderBy(org.apache.spark.sql.functions.col("occupation").desc(),
            org.apache.spark.sql.functions.col("age").asc());

    java.util.List<Row> results = new ArrayList<>();

    // Iterating through every row which is generated by previous transformation and
    // comparing with ratings dataset to generate genres ranking
    for (Row row : groupddf.collectAsList()) {
      // select all movie id's by which are rated by user group.
      Dataset<Row> usersRatingsMovieIds = ratingsdf.select("movieId")
          .where(col("userId").isin(row.getList(2).stream().toArray(Integer[]::new)));

      // Selecting all genres by joining movies data set which ever present in rating.
      Dataset<Row> genres = moviesdf
          .join(usersRatingsMovieIds,
              moviesdf.col("movieId").equalTo(usersRatingsMovieIds.col("movieId")))
          .select("genres");

      // grouping all genres and ranking them.
      Dataset<Row> topFiveGenres =
          genres.withColumn("genres", org.apache.spark.sql.functions.explode(genres.col("genres")))
              .groupBy("genres").count().orderBy(org.apache.spark.sql.functions.col("count").desc())
              .limit(5).drop("count");

      // collecting all genres
      java.util.List<String> topFiveRankings = new ArrayList<>(6);
      topFiveRankings
          .addAll(topFiveGenres.map(r -> r.mkString(), Encoders.STRING()).collectAsList());

      Row finalRow = RowFactory.create(row.get(0), row.get(1), row.get(2),
          StringUtils.join(topFiveRankings, ','));
      results.add(finalRow);
    }
    XLSXUtils.writeReport3Intoxls(results);
  }

  /**
   * Loading user's data.
   * 
   * @return
   */
  private Dataset<Row> loadUserData() {
    JavaRDD<User> users = SparkContext.getJavaSparkContext().textFile(USERS_FILE_PATH).map(

        new Function<String, User>() {
          private static final long serialVersionUID = 1L;

          public User call(String line) throws Exception {
            String[] parts = line.split("::");
            return new User(Integer.parseInt(parts[0].trim()), parts[1].trim(),
                Integer.parseInt(parts[2].trim()), occupationMap.get(parts[3]), parts[4].trim());
          }
        });
    return SparkContext.getSqlContext().createDataFrame(users, User.class);
  }

  /**
   * Loading ratings data
   * 
   * @return
   */
  private Dataset<Row> loadRatingsdata() {
    JavaRDD<Rating> ratings = SparkContext.getJavaSparkContext().textFile(RATINGS_FILE_PATH)
        .map(new Function<String, Rating>() {

          private static final long serialVersionUID = 1L;

          @Override
          public Rating call(String line) throws Exception {
            String[] parts = line.split("::");
            return new Rating(Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim()),
                Integer.parseInt(parts[2].trim()), Long.parseLong(parts[2].trim()));
          }
        });
    return SparkContext.getSqlContext().createDataFrame(ratings, Rating.class);
  }

  /**
   * Loading movies data
   * 
   * @return
   */
  private Dataset<Row> loadMoviesdata() {
    JavaRDD<Movie> movies = SparkContext.getJavaSparkContext().textFile(MOVIES_FILE_PATH)
        .map(new Function<String, Movie>() {
          private static final long serialVersionUID = 1L;

          @Override
          public Movie call(String line) throws Exception {
            String[] parts = line.split("::");
            return new Movie(Integer.parseInt(parts[0].trim()), parts[1].trim(),
                new HashSet<String>(Arrays.asList(parts[2].trim().split("\\|"))));
          }
        });
    return SparkContext.getSqlContext().createDataFrame(movies, Movie.class);
  }

  // loading occupation lookup map.
  {
    occupationMap.put("0", " other");
    occupationMap.put("1", "academic/educator");
    occupationMap.put("2", "artist");
    occupationMap.put("3", "clerical/admin");
    occupationMap.put("4", "college/grad student");
    occupationMap.put("5", "customer service");
    occupationMap.put("6", "doctor/health care");
    occupationMap.put("7", "executive/managerial");
    occupationMap.put("8", "farmer");
    occupationMap.put("9", "homemaker");
    occupationMap.put("10", "K-12 student");
    occupationMap.put("11", "lawyer");
    occupationMap.put("12", "programmer");
    occupationMap.put("13", "retired");
    occupationMap.put("14", "sales/marketing");
    occupationMap.put("15", "scientist");
    occupationMap.put("16", "self-employed");
    occupationMap.put("17", "technician/engineer");
    occupationMap.put("18", "tradesman/craftsman");
    occupationMap.put("19", "unemployed");
    occupationMap.put("20", "writer");
  }
}

abstract class MyFunction1<T, R> extends AbstractFunction1<T, R> implements Serializable {
}

