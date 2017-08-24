package com.data.movies.bean;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Muthamizhan
 *
 */
public class Movie implements Serializable {

  private static final long serialVersionUID = 1L;
  private int movieId;
  private String movieName;
  private Set<String> genres;

  public Movie(int movieId, String movieName, HashSet<String> genres) {
    super();
    this.movieId = movieId;
    this.movieName = movieName;
    this.genres = genres;
  }

  public int getMovieId() {
    return movieId;
  }

  public void setMovieId(int movieId) {
    this.movieId = movieId;
  }

  public String getMovieName() {
    return movieName;
  }

  public void setMovieName(String movieName) {
    this.movieName = movieName;
  }

  public Set<String> getGenres() {
    return genres;
  }

  public void setGenres(Set<String> generes) {
    this.genres = generes;
  }
}
