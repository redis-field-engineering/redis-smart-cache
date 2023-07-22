package com.redis.smartcache.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.sql.RowSet;

import com.redis.smartcache.core.KeyBuilder;

public class SmartPreparedStatement extends SmartStatement implements PreparedStatement {

    private static final String TYPE = "prep";

    private static final String METHOD_CANNOT_BE_USED = "Cannot use query methods that take a query string on a PreparedStatement";

    protected static final String PARAMETER_SEPARATOR = ",";

    private final SortedMap<Integer, Object> parameters = new TreeMap<>();

    public SmartPreparedStatement(SmartConnection connection, PreparedStatement statement, String sql) {
        super(connection, statement);
        init(sql);
    }

    @Override
    protected String statementType() {
        return TYPE;
    }

    @Override
    protected String key(String id) {
        KeyBuilder keyBuilder = connection.getKeyBuilder();
        String paramsId = connection.hash(keyBuilder.join(parameters()));
        return keyBuilder.build(id, paramsId);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(((PreparedStatement) statement)::executeQuery);
    }

    protected Collection<Object> parameters() {
        return parameters.values();
    }

    @Override
    public int executeUpdate() throws SQLException {
        return ((PreparedStatement) statement).executeUpdate();
    }

    @Override
    public boolean execute() throws SQLException {
        return execute(((PreparedStatement) statement)::execute);
    }

    @Override
    public RowSet executeQuery(String sql) throws SQLException {
        throw new SQLException(METHOD_CANNOT_BE_USED);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException(METHOD_CANNOT_BE_USED);
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLException(METHOD_CANNOT_BE_USED);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException(METHOD_CANNOT_BE_USED);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        ((PreparedStatement) statement).setNull(parameterIndex, sqlType);
        parameters.remove(parameterIndex);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        ((PreparedStatement) statement).setBoolean(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        ((PreparedStatement) statement).setByte(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        ((PreparedStatement) statement).setShort(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        ((PreparedStatement) statement).setInt(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        ((PreparedStatement) statement).setLong(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        ((PreparedStatement) statement).setFloat(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        ((PreparedStatement) statement).setDouble(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        ((PreparedStatement) statement).setBigDecimal(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        ((PreparedStatement) statement).setString(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        ((PreparedStatement) statement).setBytes(parameterIndex, x);
        parameters.put(parameterIndex, new String(x));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        ((PreparedStatement) statement).setDate(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        ((PreparedStatement) statement).setTime(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        ((PreparedStatement) statement).setTimestamp(parameterIndex, x);
        parameters.put(parameterIndex, x);

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ((PreparedStatement) statement).setAsciiStream(parameterIndex, x, length);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ((PreparedStatement) statement).setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ((PreparedStatement) statement).setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        ((PreparedStatement) statement).clearParameters();
        parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        ((PreparedStatement) statement).setObject(parameterIndex, x, targetSqlType);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ((PreparedStatement) statement).setObject(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void addBatch() throws SQLException {
        ((PreparedStatement) statement).addBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        ((PreparedStatement) statement).setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        ((PreparedStatement) statement).setRef(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        ((PreparedStatement) statement).setBlob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        ((PreparedStatement) statement).setClob(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        ((PreparedStatement) statement).setArray(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return ((PreparedStatement) statement).getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        ((PreparedStatement) statement).setDate(parameterIndex, x, cal);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        ((PreparedStatement) statement).setTime(parameterIndex, x, cal);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        ((PreparedStatement) statement).setTimestamp(parameterIndex, x, cal);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        ((PreparedStatement) statement).setNull(parameterIndex, sqlType, typeName);
        parameters.remove(parameterIndex);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        ((PreparedStatement) statement).setURL(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return ((PreparedStatement) statement).getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        ((PreparedStatement) statement).setRowId(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        ((PreparedStatement) statement).setNString(parameterIndex, value);
        parameters.put(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        ((PreparedStatement) statement).setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        ((PreparedStatement) statement).setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        ((PreparedStatement) statement).setClob(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        ((PreparedStatement) statement).setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        ((PreparedStatement) statement).setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        ((PreparedStatement) statement).setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        ((PreparedStatement) statement).setObject(parameterIndex, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ((PreparedStatement) statement).setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ((PreparedStatement) statement).setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        ((PreparedStatement) statement).setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        ((PreparedStatement) statement).setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        ((PreparedStatement) statement).setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        ((PreparedStatement) statement).setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        ((PreparedStatement) statement).setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        ((PreparedStatement) statement).setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        ((PreparedStatement) statement).setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        ((PreparedStatement) statement).setNClob(parameterIndex, reader);
    }

}
