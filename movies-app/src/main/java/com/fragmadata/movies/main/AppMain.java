package com.fragmadata.movies.main;

import com.fragmadata.movies.sparkcore.MoviesSparkJob;
import com.fragmadata.movies.util.XLSXUtils;

/**
 * @author Muthamizhan
 *
 */
public class AppMain {

  public static void main(String[] args) {
    try {
      System.out.println("Inilizing App !");

      System.setProperty("hadoop.home.dir", "C:/winutil/");
      String BASE_DATA_FILE_PATH = "C:/Users/Administrator/spark-project/movies-app/";

      MoviesSparkJob.USERS_FILE_PATH = BASE_DATA_FILE_PATH + "users.dat";
      MoviesSparkJob.MOVIES_FILE_PATH = BASE_DATA_FILE_PATH + "movies.dat";
      MoviesSparkJob.RATINGS_FILE_PATH = BASE_DATA_FILE_PATH + "ratings.dat";

      XLSXUtils.reportFileName = BASE_DATA_FILE_PATH + "movies-coding-problem-report.xls";

      MoviesSparkJob job = new MoviesSparkJob("local[3]", "movies-app");
      job.process();

      System.out.println("Completed processing!!!");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
