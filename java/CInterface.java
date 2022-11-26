import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Defines the interface to the calling C module
 */
public interface CInterface {

    void createConnection(int key, String[] options) throws Exception;
    void createStatement(String query) throws SQLException;
    int createStatementID(String query) throws Exception;
    void clearResultSetID(int resultSetID) throws SQLException;
    int createPreparedStatement(String query) throws Exception;
    void execPreparedStatement(int resultSetID) throws SQLException;
    Object[] getResultSet(int resultSetID) throws SQLException;
    int getNumberOfColumns(int resultSetID) throws SQLException;
    String[] getTableNames() throws SQLException;
    String[] getColumnNames(String tableName) throws SQLException;
    String[] getColumnTypes(String tableName) throws SQLException;
    String[] getPrimaryKey(String tableName) throws SQLException;
    void cancel() throws SQLException;

    void bindNullPreparedStatement(int attnum, int resultSetID) throws SQLException;
    void bindIntPreparedStatement(int values, int attnum, int resultSetID) throws SQLException;
    void bindLongPreparedStatement(long values, int attnum, int resultSetID) throws SQLException;
    void bindFloatPreparedStatement(float values, int attnum, int resultSetID) throws SQLException;
    void bindDoublePreparedStatement(double values, int attnum, int resultSetID) throws SQLException;
    void bindBooleanPreparedStatement(boolean values, int attnum, int resultSetID) throws SQLException;
    void bindStringPreparedStatement(String values, int attnum, int resultSetID) throws SQLException;
    void bindByteaPreparedStatement(byte[] dat, long length, int attnum, int resultSetID) throws SQLException;
    void bindTimePreparedStatement(String values, int attnum, int resultSetID) throws SQLException;
    void bindTimeTZPreparedStatement(String values, int attnum, int resultSetID) throws SQLException;
    void bindTimestampPreparedStatement(long usec, int attnum, int resultSetID) throws SQLException;

    String getIdentifierQuoteString() throws SQLException;


}
