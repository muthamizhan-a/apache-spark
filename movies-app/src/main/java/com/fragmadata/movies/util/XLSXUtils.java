package com.fragmadata.movies.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

/**
 * @author Muthamizhan
 *
 */
public class XLSXUtils {

  public static String reportFileName = "movies-coding-problem-report.xls";
  static HSSFWorkbook workbook;
  static FileInputStream file;
  static {
    try {
      File f = new File(reportFileName);
      if (f.exists())
        f.delete();
      f.createNewFile();
      file = new FileInputStream(f);
      workbook = new HSSFWorkbook();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writeReport3Intoxls(java.util.List<org.apache.spark.sql.Row> results) {
    try {
      HSSFSheet sheet = workbook.createSheet("GENRE RANKING");

      int rowNum = 0;
      System.out.println("-------------- Creating GENRE RANKING Report -----------------");
      Row row = sheet.createRow(rowNum++);
      Cell cell = row.createCell(0);
      cell.setCellValue((String) "OCCUPATION");
      cell = row.createCell(1);
      cell.setCellValue((String) "AGE GROUP");
      cell = row.createCell(2);
      cell.setCellValue((String) "RANK 1");
      cell = row.createCell(3);
      cell.setCellValue((String) "RANK 2");
      cell = row.createCell(4);
      cell.setCellValue((String) "RANK 3");
      cell = row.createCell(5);
      cell.setCellValue((String) "RANK 4");
      cell = row.createCell(6);
      cell.setCellValue((String) "RANK 5");
      for (org.apache.spark.sql.Row res : results) {
        row = sheet.createRow(rowNum++);
        int colNum = 0;
        cell = row.createCell(colNum++);
        cell.setCellValue((String) res.get(0));
        cell = row.createCell(colNum++);
        cell.setCellValue((String) res.get(1));
        String[] ranks = res.get(3).toString().split(",");
        for (int i = 0; i < 5; i++) {
          String val = "-";
          try {
            val = ranks[i];
          } catch (Exception e) {
          }
          cell = row.createCell(colNum++);
          cell.setCellValue((String) val);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void writeReport2Intoxls(List<String> results) {
    try {
      HSSFSheet sheet = workbook.createSheet("TOP MOST RATED MOVIES");

      int rowNum = 0;
      System.out.println("-------------- Creating TOP MOST RATED MOVIES Report -----------------");

      Row row = sheet.createRow(rowNum++);
      Cell cell = row.createCell(0);
      cell.setCellValue((String) "TOP 20 RATED MOVIES");

      for (String str : results) {
        row = sheet.createRow(rowNum++);
        int colNum = 0;
        cell = row.createCell(colNum++);
        cell.setCellValue((String) str);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void writeReport1(List<String> results) {
    try {
      HSSFSheet sheet = workbook.createSheet("TOP MOST VIEWED MOVIES");
      int rowNum = 0;
      
      System.out.println("-------------- Creating TOP MOST VIEWED MOVIES Report -----------------");

      Row row = sheet.createRow(rowNum++);
      Cell cell = row.createCell(0);
      cell.setCellValue((String) "TOP 10 VIEWED MOVIES");

      for (String res : results) {
        System.out.println(res);
        row = sheet.createRow(rowNum++);
        int colNum = 0;
        cell = row.createCell(colNum++);
        cell.setCellValue(res);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public static void flush() {
    try {
      file.close();
      FileOutputStream out = new FileOutputStream(new File(reportFileName));
      workbook.write(out);
      out.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
