package com.data.movies.bean;

import java.io.Serializable;

/**
 * @author Muthamizhan
 *
 */
public class User implements Serializable {

  private static final long serialVersionUID = 1L;
  private int userId;
  private String gender;
  private int age;
  private String occupation;
  private String zipCode;
  
  public User(int userId, String gender, int age, String occupation, String zipCode) {
    super();
    this.userId = userId;
    this.gender = gender;
    this.age = age;
    this.occupation = occupation;
    this.zipCode = zipCode;
  }

  
  public int getUserId() {
    return userId;
  }
  public void setUserId(int userId) {
    this.userId = userId;
  }
  public String getGender() {
    return gender;
  }
  public void setGender(String gender) {
    this.gender = gender;
  }
  public int getAge() {
    return age;
  }
  public void setAge(int age) {
    this.age = age;
  }
  public String getOccupation() {
    return occupation;
  }
  public void setOccupation(String occupation) {
    this.occupation = occupation;
  }
  public String getZipCode() {
    return zipCode;
  }
  public void setZipCode(String zipCode) {
    this.zipCode = zipCode;
  }
  
  
  

}
