/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper for JDBC
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Atri Sharma <atri.jiit@gmail.com>
 * Changes by: Heimir Sverrisson <heimir.sverrisson@gmail.com>, 2015-04-17
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *                jdbc_fdw/JDBCUtils.java
 *
 *-------------------------------------------------------------------------
 */

import java.io.*;
import java.net.URL;
import java.sql.*;
import java.time.LocalTime;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

public class JDBCUtils {
  private Connection conn = null;
  private static JDBCDriverLoader jdbcDriverLoader;
  private StringWriter exceptionStringWriter;
  private PrintWriter exceptionPrintWriter;
  private int queryTimeoutValue;
  private Statement tmpStmt;
  private PreparedStatement tmpPstmt;
  private static ConcurrentHashMap<Integer, Connection> ConnectionHash = new ConcurrentHashMap<Integer, Connection>();
  private static int resultSetKey = 1;
  private static ConcurrentHashMap<Integer, ResultSetInfo> resultSetInfoMap =
      new ConcurrentHashMap<Integer, ResultSetInfo>();

  /*
   * createConnection
   *      Initiates the connection to the foreign database after setting
   *      up initial configuration.
   *      key - the serverid for the connection cache identifying
   *      Caller will pass in a six element array with the following elements:
   *          0 - Driver class name, 1 - JDBC URL, 2 - Username
   *          3 - Password, 4 - Query timeout in seconds, 5 - jarfile
   *
   */
  public void createConnection(int key, String[] options) throws Exception {
    String driverClassName = options[0];
    String url = options[1];
    String userName = options[2];
    String password = options[3];
    String qTimeoutValue = options[4];
    String fileName = options[5];

    queryTimeoutValue = Integer.parseInt(qTimeoutValue);
    exceptionStringWriter = new StringWriter();
    exceptionPrintWriter = new PrintWriter(exceptionStringWriter);
    File JarFile = new File(fileName);
    String jarfile_path = JarFile.toURI().toURL().toString();
    if (jdbcDriverLoader == null) {
      /* If jdbcDriverLoader is being created. */
      jdbcDriverLoader = new JDBCDriverLoader(new URL[] {JarFile.toURI().toURL()});
    } else if (jdbcDriverLoader.CheckIfClassIsLoaded(driverClassName) == null) {
      jdbcDriverLoader.addPath(jarfile_path);
    }
    Class jdbcDriverClass = jdbcDriverLoader.loadClass(driverClassName);
    Driver jdbcDriver = (Driver) jdbcDriverClass.newInstance();
    Properties jdbcProperties = new Properties();
    jdbcProperties.put("user", userName);
    jdbcProperties.put("password", password);
    /* get connection from cache */
    if (ConnectionHash.containsKey(key)) {
      conn = ConnectionHash.get(key);
    }
    if (conn == null) {
      conn = jdbcDriver.connect(url, jdbcProperties);
      ConnectionHash.put(key, conn);
    }
  }

  /*
   * createStatement
   *      Create a statement object based on the query
   */
  public void createStatement(String query) throws SQLException {
    /*
     *  Set the query select all columns for creating the same size of the result table
     *  because jvm can only return String[] - resultRow.
     *  Todo: return only necessary column.
     */
    assertConnExists();
    tmpStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    if (queryTimeoutValue != 0) {
      tmpStmt.setQueryTimeout(queryTimeoutValue);
    }
    tmpStmt.executeQuery(query);
  }

  /*
   * createStatementID
   *      Create a statement object based on the query
   *      with a specific resultID and return back to the calling C function
   *      Returns:
   *          resultID on success
   */
  public int createStatementID(String query) throws Exception {
    assertConnExists();
    tmpStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    if (queryTimeoutValue != 0) {
      tmpStmt.setQueryTimeout(queryTimeoutValue);
    }
    ResultSet tmpResultSet = tmpStmt.executeQuery(query);
    ResultSetMetaData rSetMetadata = tmpResultSet.getMetaData();
    int tmpNumberOfColumns = rSetMetadata.getColumnCount();
    int tmpResultSetKey = initResultSetKey();
    resultSetInfoMap.put(
        tmpResultSetKey,
        new ResultSetInfo(
            tmpResultSet, tmpNumberOfColumns, 0, null));
    return tmpResultSetKey;
  }

  /*
   * clearResultSetID
   *      clear ResultSetID
   */
  public void clearResultSetID(int resultSetID) throws SQLException {
    assertConnExists();
    resultSetInfoMap.remove(resultSetID);
  }

  /*
   * createPreparedStatement
   *      Create a PreparedStatement object based on the query
   *      with a specific resultID and return back to the calling C function
   *      Returns:
   *          resultID on success
   */
  public int createPreparedStatement(String query) throws Exception {
    assertConnExists();
    PreparedStatement tmpPstmt = (PreparedStatement) conn.prepareStatement(query);
    if (queryTimeoutValue != 0) {
      tmpPstmt.setQueryTimeout(queryTimeoutValue);
    }
    int tmpResultSetKey = initResultSetKey();
    resultSetInfoMap.put(tmpResultSetKey, new ResultSetInfo(null, null, 0, tmpPstmt));
    return tmpResultSetKey;
  }

  /*
   * execPreparedStatement
   *      Create a PreparedStatement object based on the query
   */
  public void execPreparedStatement(int resultSetID) throws SQLException {
    assertConnExists();
    PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(tmpPstmt);
    int tmpNumberOfAffectedRows = tmpPstmt.executeUpdate();
    tmpPstmt.clearParameters();

    resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    resultSetInfoMap.get(resultSetID).setNumberOfAffectedRows(tmpNumberOfAffectedRows);
  }

  /*
   * getNumberOfColumns
   *      Returns arrayOfNumberOfColumns[resultSetID]
   *      Returns:
   *          NumberOfColumns on success
   */
  public int getNumberOfColumns(int resultSetID) throws SQLException {
    return resultSetInfoMap.get(resultSetID).getNumberOfColumns();
  }

  /*
   * getNumberOfAffectedRows
   *      Returns numberOfAffectedRows
   */
  public int getNumberOfAffectedRows(int resultSetID) throws SQLException {
    return resultSetInfoMap.get(resultSetID).getNumberOfAffectedRows();
  }

  /*
   * getResultSet
   *      Returns the result set that is returned from the foreign database
   *      after execution of the query to C code. One row is returned at a time
   *      as an Object array. For binary related types (BINARY, LONGVARBINARY, VARBINARY,
   *      BLOB), Object corresponds to byte array. For other types, Object corresponds to
   *      String. After last row null is returned.
   */
  public Object[] getResultSet(int resultSetID) throws SQLException {
    try {
      ResultSet tmpResultSet = resultSetInfoMap.get(resultSetID).getResultSet();
      int tmpNumberOfColumns = resultSetInfoMap.get(resultSetID).getNumberOfColumns();
      Object[] tmpArrayOfResultRow = new Object[tmpNumberOfColumns];
      ResultSetMetaData mtData = tmpResultSet.getMetaData();

      /* Row-by-row processing is done in jdbc_fdw.One row
       * at a time is returned to the C code. */
      if (tmpResultSet.next()) {
        for (int i = 0; i < tmpNumberOfColumns; i++) {
          int columnType = mtData.getColumnType(i + 1);

          switch (columnType) {
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
            case Types.BLOB:
              /* Get byte array */
              tmpArrayOfResultRow[i] = tmpResultSet.getBytes(i + 1);
              break;
            case Types.TIMESTAMP:
              /*
               * Get the timestamp in UTC time zone by default
               * to avoid being affected by the remote server's time zone.
               */
              java.util.Calendar cal = Calendar.getInstance();
              cal.setTimeZone(TimeZone.getTimeZone("UTC"));
              Timestamp resTimestamp = tmpResultSet.getTimestamp(i + 1, cal);
              if (resTimestamp != null) {
                /* Timestamp is returned as text in ISO 8601 style */
                tmpArrayOfResultRow[i] = resTimestamp.toInstant().toString();
              } else {
                tmpArrayOfResultRow[i] = null;
              }
              break;
            default:
              /* Convert all columns to String */
              tmpArrayOfResultRow[i] = tmpResultSet.getString(i + 1);
          }
        }
        /* The current row in resultSet is returned
         * to the C code in a Java String array that
         * has the value of the fields of the current
         * row as it values. */
        return tmpArrayOfResultRow;
      } else {
        /*
         * All of resultSet's rows have been returned to the C code.
         * Close tmpResultSet's statement
         */
        tmpResultSet.getStatement().close();
        clearResultSetID(resultSetID);
        return null;
      }
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * getTableNames
   *      Returns the column name
   */
  public String[] getTableNames() throws SQLException {
    assertConnExists();
    DatabaseMetaData md = conn.getMetaData();
    ResultSet tmpResultSet = md.getTables(null, null, "%", null);

    List<String> tmpTableNamesList = new ArrayList<String>();
    while (tmpResultSet.next()) {
      tmpTableNamesList.add(tmpResultSet.getString(3));
    }
    String[] tmpTableNames = new String[tmpTableNamesList.size()];
    for (int i = 0; i < tmpTableNamesList.size(); i++) {
      tmpTableNames[i] = tmpTableNamesList.get(i);
    }
    return tmpTableNames;
  }

  /*
   * getColumnNames
   *      Returns the column names
   */
  public String[] getColumnNames(String tableName) throws SQLException {
    assertConnExists();
    DatabaseMetaData md = conn.getMetaData();
    ResultSet tmpResultSet = md.getColumns(null, null, tableName, null);
    List<String> tmpColumnNamesList = new ArrayList<String>();
    while (tmpResultSet.next()) {
      tmpColumnNamesList.add(tmpResultSet.getString("COLUMN_NAME"));
    }
    String[] tmpColumnNames = new String[tmpColumnNamesList.size()];
    for (int i = 0; i < tmpColumnNamesList.size(); i++) {
      tmpColumnNames[i] = tmpColumnNamesList.get(i);
    }
    return tmpColumnNames;
  }

  /*
   * getColumnTypes
   *      Returns the column name
   */
  public String[] getColumnTypes(String tableName) throws SQLException {
    assertConnExists();
    DatabaseMetaData md = conn.getMetaData();
    ResultSet tmpResultSet = md.getColumns(null, null, tableName, null);
    ResultSetMetaData rSetMetadata = tmpResultSet.getMetaData();
    List<String> tmpColumnTypesList = new ArrayList<String>();
    while (tmpResultSet.next()) {
      tmpColumnTypesList.add(tmpResultSet.getString("TYPE_NAME"));
    }
    String[] tmpColumnTypes = new String[tmpColumnTypesList.size()];
    for (int i = 0; i < tmpColumnTypesList.size(); i++) {
      switch (tmpColumnTypesList.get(i)) {
        case "BYTE":
        case "SHORT":
          tmpColumnTypes[i] = "SMALLINT";
          break;
        case "LONG":
          tmpColumnTypes[i] = "BIGINT";
          break;
        case "CHAR":
          tmpColumnTypes[i] = "CHAR (1)";
          break;
        case "STRING":
          tmpColumnTypes[i] = "TEXT";
          break;
        case "FLOAT":
          tmpColumnTypes[i] = "FLOAT4";
          break;
        case "DOUBLE":
          tmpColumnTypes[i] = "FLOAT8";
          break;
        case "BLOB":
          tmpColumnTypes[i] = "BYTEA";
          break;
        case "BOOL_ARRAY":
          tmpColumnTypes[i] = "BOOL[]";
          break;
        case "STRING_ARRAY":
          tmpColumnTypes[i] = "TEXT[]";
          break;
        case "BYTE_ARRAY":
        case "SHORT_ARRAY":
          tmpColumnTypes[i] = "SMALLINT[]";
          break;
        case "INTEGER_ARRAY":
          tmpColumnTypes[i] = "INTEGER[]";
          break;
        case "LONG_ARRAY":
          tmpColumnTypes[i] = "BIGINT[]";
          break;
        case "FLOAT_ARRAY":
          tmpColumnTypes[i] = "FLOAT4[]";
          break;
        case "DOUBLE_ARRAY":
          tmpColumnTypes[i] = "FLOAT8[]";
          break;
        case "TIMESTAMP_ARRAY":
          tmpColumnTypes[i] = "TIMESTAMP[]";
          break;
        default:
          tmpColumnTypes[i] = tmpColumnTypesList.get(i);
      }
    }
    return tmpColumnTypes;
  }

  /*
   * getPrimaryKey
   *      Returns the column name
   */
  public String[] getPrimaryKey(String tableName) throws SQLException {
    assertConnExists();
    DatabaseMetaData md = conn.getMetaData();
    ResultSet tmpResultSet = md.getPrimaryKeys(null, null, tableName);
    ResultSetMetaData rSetMetadata = tmpResultSet.getMetaData();
    List<String> tmpPrimaryKeyList = new ArrayList<String>();
    while (tmpResultSet.next()) {
      tmpPrimaryKeyList.add(tmpResultSet.getString("COLUMN_NAME"));
    }
    String[] tmpPrimaryKey = new String[tmpPrimaryKeyList.size()];
    for (int i = 0; i < tmpPrimaryKeyList.size(); i++) {
      tmpPrimaryKey[i] = tmpPrimaryKeyList.get(i);
    }
    return tmpPrimaryKey;
  }

  /*
   * closeStatement
   *      Releases the resources used by statement. Keeps the connection
   *      open for another statement to be executed.
   */
  public void closeStatement() throws SQLException {
    resultSetInfoMap.clear();

    if (tmpStmt != null) {
      tmpStmt.close();
      tmpStmt = null;
    }
    if (tmpPstmt != null) {
      tmpPstmt.close();
      tmpPstmt = null;
    }
  }

  /*
   * closeConnection
   *     Releases the resources used by connection.
   */
  public void closeConnection() throws SQLException {
    closeStatement();
    if (conn != null) {
      conn.close();
      conn = null;
    }
  }

  /*
   * cancel
   *      Cancels the query and releases the resources in case query
   *      cancellation is requested by the user.
   */
  public void cancel() throws SQLException {
    closeStatement();
  }

  /*
   * assertConnExists
   *      Check the connection exists or not.
   *      throw error message when the connection dosn't exist.
   */
  public void assertConnExists() throws IllegalArgumentException {
    if (conn == null) {
      throw new IllegalArgumentException(
          "Must create connection before creating a prepared statment");
    }
  }

  /*
   * assertStatementNotNull
   *      Check the Prepared Statement exists or not.
   *      throw error message when the Prepared Statement doesn't exist.
   */
  public void assertStatementNotNull(PreparedStatement pstmt) throws IllegalArgumentException {
    if (pstmt == null) {
      throw new IllegalArgumentException(
          "Must create a prior prepared statement before execute it");
    }
  }

  /*
   * bindNullPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindNullPreparedStatement(int attnum, int resultSetID) throws SQLException {
    assertConnExists();
    PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(tmpPstmt);
    tmpPstmt.setNull(attnum, Types.NULL);
    resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
  }

  /*
   * bindIntPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindIntPreparedStatement(int values, int attnum, int resultSetID)
      throws SQLException {
    assertConnExists();
    PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(tmpPstmt);
    tmpPstmt.setInt(attnum, values);
    resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
  }

  /*
   * bindLongPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindLongPreparedStatement(long values, int attnum, int resultSetID)
      throws SQLException {
    assertConnExists();
    PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(tmpPstmt);
    tmpPstmt.setLong(attnum, values);
    resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
  }

  /*
   * bindFloatPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindFloatPreparedStatement(float values, int attnum, int resultSetID)
      throws SQLException {

    assertConnExists();
    PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(tmpPstmt);
    tmpPstmt.setFloat(attnum, values);
    resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
  }

  /*
   * bindDoublePreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindDoublePreparedStatement(double values, int attnum, int resultSetID)
      throws SQLException {
    assertConnExists();
    PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(tmpPstmt);
    tmpPstmt.setDouble(attnum, values);
    resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
  }

  /*
   * bindBooleanPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindBooleanPreparedStatement(boolean values, int attnum, int resultSetID)
      throws SQLException {
    assertConnExists();
    PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(tmpPstmt);
    tmpPstmt.setBoolean(attnum, values);
    resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
  }

  /*
   * bindStringPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindStringPreparedStatement(String values, int attnum, int resultSetID)
      throws SQLException {
    assertConnExists();
    PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(tmpPstmt);
    tmpPstmt.setString(attnum, values);
    resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
  }

  /*
   * bindByteaPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindByteaPreparedStatement(byte[] dat, long length, int attnum, int resultSetID)
      throws SQLException {
    assertConnExists();
    PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(tmpPstmt);
    InputStream targetStream = new ByteArrayInputStream(dat);
    tmpPstmt.setBinaryStream(attnum, targetStream, length);
    resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
  }

  /*
   * bindTimePreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindTimePreparedStatement(String values, int attnum, int resultSetID)
      throws SQLException {

    assertConnExists();
    PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(tmpPstmt);
    String pattern = "[HH:mm:ss][.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S][z][XXX][X]";
    LocalTime localTime = LocalTime.parse(values, DateTimeFormatter.ofPattern(pattern));
    tmpPstmt.setObject(attnum, localTime);
    resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
  }

  /*
   * bindTimeTZPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   *      set with localtime: might lost time-zone
   */
  public void bindTimeTZPreparedStatement(String values, int attnum, int resultSetID)
      throws SQLException {
    assertConnExists();
    PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(tmpPstmt);

    String pattern = "[HH:mm:ss][.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S][z][XXX][X]";
    LocalTime localTime = LocalTime.parse(values, DateTimeFormatter.ofPattern(pattern));
    tmpPstmt.setObject(attnum, localTime);
    resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
  }

  /*
   * Set timestamp to prepared statement
   * Use the UTC time zone as default to avoid being affected by the JVM time zone
   */
  private void setTimestamp(PreparedStatement preparedStatement, int attnum, Timestamp timestamp)
    throws SQLException {
      java.util.Calendar cal = Calendar.getInstance();
      cal.setTimeZone(TimeZone.getTimeZone("UTC"));
      try {
        /* Specify time zone (cal) if possible */
        preparedStatement.setTimestamp(attnum, timestamp, cal);
      } catch (SQLFeatureNotSupportedException e) {
        /* GridDB only, no calendar support in setTimestamp() */
        preparedStatement.setTimestamp(attnum, timestamp);
      }
    }

  /*
   * bindTimestampPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindTimestampPreparedStatement(long usec, int attnum, int resultSetID)
    throws SQLException {
    assertConnExists();
    PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(tmpPstmt);
    Instant instant = Instant.EPOCH.plus(usec, ChronoUnit.MICROS);
    Timestamp timestamp = Timestamp.from(instant);
    setTimestamp(tmpPstmt, attnum, timestamp);
    resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
  }

  /*
   * Avoid race case.
   */
  synchronized public int initResultSetKey() throws Exception{
    int datum = this.resultSetKey;
    while (resultSetInfoMap.containsKey(this.resultSetKey)) {
      /* avoid giving minus key */
      if (this.resultSetKey == Integer.MAX_VALUE) {
        this.resultSetKey = 1;
      }
      this.resultSetKey++;
      /* resultSetKey full */
      if (this.resultSetKey == datum) {
        throw new SQLException("resultSetKey is full");
      }
    }
    return this.resultSetKey;
  }

  /*
   * Get identifier quote char from remote server
   */
  public String getIdentifierQuoteString() throws SQLException{
    assertConnExists();
    DatabaseMetaData md = conn.getMetaData();
    return md.getIdentifierQuoteString();
  }
}
