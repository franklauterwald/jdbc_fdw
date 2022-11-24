/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper for JDBC
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * This software is released under the PostgreSQL Licence
 *
 * IDENTIFICATION
 *                jdbc_fdw/resultSetInfo.java
 *
 *-------------------------------------------------------------------------
 */
import java.sql.*;
import java.util.*;

public class ResultSetInfo {
  private ResultSet resultSet;
  private int numberOfColumns;
  private int numberOfAffectedRows;
  private PreparedStatement pstmt;

  public ResultSetInfo(
      ResultSet resultSet,
      int numberOfColumns,
      int numberOfAffectedRows,
      PreparedStatement pstmt) {
    this.resultSet = resultSet;
    this.numberOfColumns = numberOfColumns;
    this.numberOfAffectedRows = numberOfAffectedRows;
    this.pstmt = pstmt;
  }

  public void setNumberOfAffectedRows(int fieldNumberOfAffectedRows) {
    this.numberOfAffectedRows = fieldNumberOfAffectedRows;
  }

  public ResultSet getResultSet() {
    return resultSet;
  }

  public int getNumberOfColumns() {
    return numberOfColumns;
  }

  public PreparedStatement getPstmt() {
    return pstmt;
  }
}
