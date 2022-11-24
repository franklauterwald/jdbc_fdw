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

public class JDBCUtils implements CInterface {
  private Connection conn = null;
  private static JDBCDriverLoader jdbcDriverLoader;
  private int queryTimeoutValue;
  private Statement globalStmt;
  private static ConcurrentHashMap<Integer, Connection> connectionHash = new ConcurrentHashMap<Integer, Connection>();
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
   */
  @Override
  public void createConnection(int key, String[] options) throws Exception {
    String driverClassName = options[0];
    String url = options[1];
    String userName = options[2];
    String password = options[3];
    queryTimeoutValue = Integer.parseInt(options[4]);
    String fileName = options[5];

    File JarFile = new File(fileName);
    String jarfile_path = JarFile.toURI().toURL().toString();
    if (jdbcDriverLoader == null) {
      /* If jdbcDriverLoader is being created. */
      jdbcDriverLoader = new JDBCDriverLoader(new URL[] { JarFile.toURI().toURL() });
    } else if (! jdbcDriverLoader.isClassLoaded(driverClassName)) {
      jdbcDriverLoader.addPath(jarfile_path);
    }
    Class jdbcDriverClass = jdbcDriverLoader.loadClass(driverClassName);
    Driver jdbcDriver = (Driver) jdbcDriverClass.newInstance();
    Properties jdbcProperties = new Properties();
    jdbcProperties.put("user", userName);
    jdbcProperties.put("password", password);
    /* get connection from cache */
    if (connectionHash.containsKey(key)) {
      conn = connectionHash.get(key);
    }
    if (conn == null) {
      conn = jdbcDriver.connect(url, jdbcProperties);
      connectionHash.put(key, conn);
    }
  }

  /*
   * createStatement
   *      Create a statement object based on the query
   */
  @Override
  public void createStatement(String query) throws SQLException {
    /*
     *  Set the query select all columns for creating the same size of the result table
     *  because jvm can only return String[] - resultRow.
     *  Todo: return only necessary column.
     */
    assertConnExists();
    globalStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    if (queryTimeoutValue != 0) {
      globalStmt.setQueryTimeout(queryTimeoutValue);
    }
    globalStmt.executeQuery(query);
  }

  /*
   * createStatementID
   *      Create a statement object based on the query
   *      with a specific resultID and return back to the calling C function
   *      Returns:
   *          resultID on success
   * TODO: merge with createStatement
   */
  @Override
  public int createStatementID(String query) throws Exception {
    assertConnExists();
    globalStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    if (queryTimeoutValue != 0) {
      globalStmt.setQueryTimeout(queryTimeoutValue);
    }
    ResultSet rs = globalStmt.executeQuery(query);
    ResultSetMetaData rsMetadata = rs.getMetaData();
    int numberOfColumns = rsMetadata.getColumnCount();
    int resultSetKey = initResultSetKey();
    resultSetInfoMap.put(
        resultSetKey,
        new ResultSetInfo( rs, numberOfColumns, 0, null));
    return resultSetKey;
  }

  /*
   * clearResultSetID
   *      clear ResultSetID
   */
  @Override
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
  @Override
  public int createPreparedStatement(String query) throws Exception {
    assertConnExists();
    PreparedStatement stmt = conn.prepareStatement(query);
    if (queryTimeoutValue != 0) {
      stmt.setQueryTimeout(queryTimeoutValue);
    }
    int resultSetKey = initResultSetKey();
    resultSetInfoMap.put(resultSetKey, new ResultSetInfo(null, 0, 0, stmt));
    return resultSetKey;
  }

  /*
   * execPreparedStatement
   *      Execute a PreparedStatement object based on the query
   */
  @Override
  public void execPreparedStatement(int resultSetID) throws SQLException {
    PreparedStatement stmt = getValidatedStatement(resultSetID);
    int numAffectedRows = stmt.executeUpdate();
    stmt.clearParameters();
    resultSetInfoMap.get(resultSetID).setNumberOfAffectedRows(numAffectedRows);
  }

  /*
   * getNumberOfColumns
   *      Returns arrayOfNumberOfColumns[resultSetID]
   *      Returns:
   *          NumberOfColumns on success
   */
  @Override
  public int getNumberOfColumns(int resultSetID) throws SQLException {
    return resultSetInfoMap.get(resultSetID).getNumberOfColumns();
  }

  /*
   * getResultSet
   *      Returns the result set that is returned from the foreign database
   *      after execution of the query to C code. One row is returned at a time
   *      as an Object array. For binary related types (BINARY, LONGVARBINARY, VARBINARY,
   *      BLOB), Object corresponds to byte array. For other types, Object corresponds to
   *      String. After last row null is returned.
   *
   */
  @Override
  public Object[] getResultSet(int resultSetID) throws SQLException {
    ResultSet rs = resultSetInfoMap.get(resultSetID).getResultSet();
    int numCols = resultSetInfoMap.get(resultSetID).getNumberOfColumns();
    Object[] resultRowArray = new Object[numCols];
    ResultSetMetaData mtData = rs.getMetaData();

    /* Row-by-row processing is done in jdbc_fdw.One row
     * at a time is returned to the C code. */
    if (rs.next()) {
      for (int i = 0; i < numCols; i++) {
        int columnType = mtData.getColumnType(i + 1);

        switch (columnType) {
          case Types.BINARY:
          case Types.LONGVARBINARY:
          case Types.VARBINARY:
          case Types.BLOB:
            /* Get byte array */
            resultRowArray[i] = rs.getBytes(i + 1);
            break;
          case Types.TIMESTAMP:
            /*
             * Get the timestamp in UTC time zone by default
             * to avoid being affected by the remote server's time zone.
             */
            java.util.Calendar cal = Calendar.getInstance();
            cal.setTimeZone(TimeZone.getTimeZone("UTC"));
            Timestamp resTimestamp = rs.getTimestamp(i + 1, cal);
            if (resTimestamp != null) {
              /* Timestamp is returned as text in ISO 8601 style */
              resultRowArray[i] = resTimestamp.toInstant().toString();
            } else {
              resultRowArray[i] = null;
            }
            break;
          default:
            /* Convert all columns to String */
            resultRowArray[i] = rs.getString(i + 1);
        }
      }
      /* The current row in resultSet is returned
       * to the C code in a Java String array that
       * has the value of the fields of the current
       * row as it values. */
      return resultRowArray;
    } else {
      /*
       * All of ResultSet's rows have been returned to the C code.
       * Close ResultSet's statement
       */
      rs.getStatement().close();
      clearResultSetID(resultSetID);
      return null;
    }
  }

  /*
   * getTableNames
   *      Returns the column name
   */
  @Override
  public String[] getTableNames() throws SQLException {
    assertConnExists();
    DatabaseMetaData md = conn.getMetaData();
    ResultSet rs = md.getTables(null, null, "%", null);
    List<String> tableNamesList = new ArrayList<String>();
    while (rs.next()) {
      tableNamesList.add(rs.getString(3));
    }
    return (String[]) tableNamesList.toArray();
  }

  /*
   * getColumnNames
   *      Returns the column names
   */
  @Override
  public String[] getColumnNames(String tableName) throws SQLException {
    assertConnExists();
    DatabaseMetaData md = conn.getMetaData();
    ResultSet rs = md.getColumns(null, null, tableName, null);
    List<String> columnNamesList = new ArrayList<String>();
    while (rs.next()) {
      columnNamesList.add(rs.getString("COLUMN_NAME"));
    }
    return (String[]) columnNamesList.toArray();
  }

  /*
   * getColumnTypes
   *      Returns the column types
   */
  @Override
  public String[] getColumnTypes(String tableName) throws SQLException {
    assertConnExists();
    DatabaseMetaData md = conn.getMetaData();
    ResultSet rs = md.getColumns(null, null, tableName, null);
    ResultSetMetaData rsMeta = rs.getMetaData();
    List<String> columnTypesList = new ArrayList<String>();
    while (rs.next()) {
      columnTypesList.add(mapJdbcToPostgresType(rs.getString("TYPE_NAME")));
    }
    return (String[]) columnTypesList.toArray();
  }

  /*
   * getPrimaryKey
   *      Returns the column name
   */
  @Override
  public String[] getPrimaryKey(String tableName) throws SQLException {
    assertConnExists();
    DatabaseMetaData md = conn.getMetaData();
    ResultSet rs = md.getPrimaryKeys(null, null, tableName);
    List<String> primaryKey = new ArrayList<String>();
    while (rs.next()) {
      primaryKey.add(rs.getString("COLUMN_NAME"));
    }
    return (String[]) primaryKey.toArray();
  }

  /*
   * closeStatement
   *      Releases the resources used by statement. Keeps the connection
   *      open for another statement to be executed.
   */
  protected void closeStatement() throws SQLException {
    resultSetInfoMap.clear();
    if (globalStmt != null) {
      globalStmt.close();
      globalStmt = null;
    }
  }

  /*
   * cancel
   *      Cancels the query and releases the resources in case query
   *      cancellation is requested by the user.
   */
  @Override
  public void cancel() throws SQLException {
    closeStatement();
  }

  /*
   * assertConnExists
   *      Check the connection exists or not.
   *      throw error message when the connection dosn't exist.
   */
  protected void assertConnExists() throws IllegalArgumentException {
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
  protected void assertStatementNotNull(PreparedStatement pstmt) throws IllegalArgumentException {
    if (pstmt == null) {
      throw new IllegalArgumentException(
          "Must create a prior prepared statement before executing it");
    }
  }

  protected PreparedStatement getValidatedStatement(int resultSetID) {
    assertConnExists();
    PreparedStatement stmt = resultSetInfoMap.get(resultSetID).getPstmt();
    assertStatementNotNull(stmt);
    return stmt;
  }

  @Override
  public void bindNullPreparedStatement(int attnum, int resultSetID) throws SQLException {
    getValidatedStatement(resultSetID).setNull(attnum, Types.NULL);
  }

  @Override
  public void bindIntPreparedStatement(int value, int attnum, int resultSetID)
      throws SQLException {
    getValidatedStatement(resultSetID).setInt(attnum, value);
  }

  @Override
  public void bindLongPreparedStatement(long value, int attnum, int resultSetID)
      throws SQLException {
    getValidatedStatement(resultSetID).setLong(attnum, value);
  }

  @Override
  public void bindFloatPreparedStatement(float value, int attnum, int resultSetID)
      throws SQLException {
    getValidatedStatement(resultSetID).setFloat(attnum, value);
  }

  @Override
  public void bindDoublePreparedStatement(double value, int attnum, int resultSetID)
      throws SQLException {
    getValidatedStatement(resultSetID).setDouble(attnum, value);
  }

  @Override
  public void bindBooleanPreparedStatement(boolean value, int attnum, int resultSetID)
      throws SQLException {
    getValidatedStatement(resultSetID).setBoolean(attnum, value);
  }

  @Override
  public void bindStringPreparedStatement(String value, int attnum, int resultSetID)
      throws SQLException {
    getValidatedStatement(resultSetID).setString(attnum, value);;
  }

  @Override
  public void bindByteaPreparedStatement(byte[] dat, long length, int attnum, int resultSetID)
      throws SQLException {
    InputStream targetStream = new ByteArrayInputStream(dat);
    getValidatedStatement(resultSetID).setBinaryStream(attnum, targetStream, length);
  }

  @Override
  public void bindTimePreparedStatement(String value, int attnum, int resultSetID)
      throws SQLException {
    String pattern = "[HH:mm:ss][.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S][z][XXX][X]";
    LocalTime localTime = LocalTime.parse(value, DateTimeFormatter.ofPattern(pattern));
    getValidatedStatement(resultSetID).setObject(attnum, localTime);
  }

  /*
   * bindTimeTZPreparedStatement
   *      TODO: timezone is ignored and local is used
   *      set with localtime: might lost time-zone
   */
  @Override
  public void bindTimeTZPreparedStatement(String value, int attnum, int resultSetID)
      throws SQLException {
    bindTimePreparedStatement(value, attnum, resultSetID);
  }

  /*
   * Set timestamp to prepared statement
   * Use the UTC time zone as default to avoid being affected by the JVM time zone
   */
  private void setTimestamp(PreparedStatement stmt, int attnum, Timestamp timestamp)
    throws SQLException {
      java.util.Calendar cal = Calendar.getInstance();
      cal.setTimeZone(TimeZone.getTimeZone("UTC"));
      try {
        /* Specify time zone (cal) if possible */
        stmt.setTimestamp(attnum, timestamp, cal);
      } catch (SQLFeatureNotSupportedException e) {
        /* GridDB only, no calendar support in setTimestamp() */
        stmt.setTimestamp(attnum, timestamp);
      }
    }

  @Override
  public void bindTimestampPreparedStatement(long usec, int attnum, int resultSetID)
    throws SQLException {
    Instant instant = Instant.EPOCH.plus(usec, ChronoUnit.MICROS);
    Timestamp timestamp = Timestamp.from(instant);
    setTimestamp(getValidatedStatement(resultSetID), attnum, timestamp);
  }

  /*
   * Avoid race case.
   */
  synchronized protected int initResultSetKey() throws Exception {
    int datum = this.resultSetKey;
    while (resultSetInfoMap.containsKey(this.resultSetKey)) {
      /* avoid giving minus key */
      if (this.resultSetKey == Integer.MAX_VALUE) {
        this.resultSetKey = 1;
      }
      this.resultSetKey++;
    }
    return this.resultSetKey;
  }

  /*
   * Get identifier quote char from remote server
   */
  @Override
  public String getIdentifierQuoteString() throws SQLException {
    assertConnExists();
    return conn.getMetaData().getIdentifierQuoteString();
  }

  private String mapJdbcToPostgresType(String in) {
    switch (in) {
      case "BYTE":
      case "SHORT"           : return "SMALLINT";
      case "LONG"            : return "BIGINT";
      case "CHAR"            : return "CHAR (1)";
      case "STRING"          : return "TEXT";
      case "FLOAT"           : return "FLOAT4";
      case "DOUBLE"          : return "FLOAT8";
      case "BLOB"            : return "BYTEA";
      case "BOOL_ARRAY"      : return "BOOL[]";
      case "STRING_ARRAY"    : return "TEXT[]";
      case "BYTE_ARRAY":
      case "SHORT_ARRAY"     : return "SMALLINT[]";
      case "INTEGER_ARRAY"   : return "INTEGER[]";
      case "LONG_ARRAY"      : return "BIGINT[]";
      case "FLOAT_ARRAY"     : return "FLOAT4[]";
      case "DOUBLE_ARRAY"    : return "FLOAT8[]";
      case "TIMESTAMP_ARRAY" : return "TIMESTAMP[]";
      default: return in;
    }
  }

}
