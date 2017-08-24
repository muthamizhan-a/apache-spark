package com.fragmadata.movies.context;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * @author Muthamizhan
 * 
 *         Singleton spark context class to hold single spark context instance to serve throughout
 *         application.
 *
 */
public class SparkContext {
  private static JavaSparkContext sc = null;
  private static SparkContext instance = null;
  private static SQLContext sqlContext = null;

  private SparkContext(String masterType, String appName) throws Exception {
    if (!StringUtils.isNotEmpty(masterType) || !StringUtils.isNotEmpty(appName)) {
      throw new Exception("ERROR: Invalid input!!");
    }
    SparkConf conf = new SparkConf().setMaster(masterType).setAppName(appName);
    sc = new JavaSparkContext(conf);
  }

  @SuppressWarnings("deprecation")
  public static SQLContext getSqlContext() {
    if (sqlContext == null && sc != null)
      sqlContext = new SQLContext(sc);
    return sqlContext;
  }

  public static void initializeJavaSparkContext(String masterType, String appName)
      throws Exception {
    if (instance == null)
      instance = new SparkContext(masterType, appName);
  }

  public static JavaSparkContext getJavaSparkContext() {
    return sc;
  }
}
