package com.data.movies.bean;

import java.io.Serializable;

/**
 * @author Muthamizhan
 *
 */
public class Rating implements Serializable{

  private static final long serialVersionUID = 3996652403815571437L;
  private int userId;
  private int movieId;
  private int ratings;
  private long timeStamp;
  
  public Rating(int userId, int movieId, int ratings, long timeStamp) {
    super();
    this.userId = userId;
    this.movieId = movieId;
    this.ratings = ratings;
    this.timeStamp = timeStamp;
  }
 
  public int getUserId() {
    return userId;
  }
  public void setUserId(int userId) {
    this.userId = userId;
  }
  public int getMovieId() {
    return movieId;
  }
  public void setMovieId(int movieId) {
    this.movieId = movieId;
  }

  public int getRatings() {
    return ratings;
  }
  public void setRatings(int ratings) {
    this.ratings = ratings;
  }
  public long getTimeStamp() {
    return timeStamp;
  }
  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }
}
