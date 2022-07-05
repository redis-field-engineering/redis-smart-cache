package com.redis.sidecar.jdbc;

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
import java.util.SortedMap;
import java.util.TreeMap;

import io.micrometer.core.instrument.MeterRegistry;

public class SidecarPreparedStatement extends SidecarStatement implements PreparedStatement {

	private static final String METHOD_CANNOT_BE_USED = "Query methods that take a query string cannot be used on a PreparedStatement";
	private final SortedMap<Integer, String> parameters = new TreeMap<>();
	private final PreparedStatement statement;

	protected SidecarPreparedStatement(SidecarConnection connection, PreparedStatement statement,
			MeterRegistry meterRegistry, String sql) {
		super(connection, statement, meterRegistry, sql);
		this.statement = statement;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
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
	protected String key() {
		return appendParameters(new StringBuilder(super.key())).toString();
	}

	protected StringBuilder appendParameters(StringBuilder stringBuilder) {
		parameters.forEach((k, v) -> appendParameter(stringBuilder, v));
		return stringBuilder;
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		return executeQuery(statement::executeQuery);
	}

	@Override
	public int executeUpdate() throws SQLException {
		return statement.executeUpdate();
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		statement.setNull(parameterIndex, sqlType);
		parameters.remove(parameterIndex);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		statement.setBoolean(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		statement.setByte(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		statement.setShort(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		statement.setInt(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		statement.setLong(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		statement.setFloat(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		statement.setDouble(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		statement.setBigDecimal(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		statement.setString(parameterIndex, x);
		parameters.put(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		statement.setBytes(parameterIndex, x);
		parameters.put(parameterIndex, new String(x));
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		statement.setDate(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		statement.setTime(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		statement.setTimestamp(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));

	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		statement.setAsciiStream(parameterIndex, x, length);
	}

	@SuppressWarnings("deprecation")
	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		statement.setUnicodeStream(parameterIndex, x, length);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		statement.setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void clearParameters() throws SQLException {
		statement.clearParameters();
		parameters.clear();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		statement.setObject(parameterIndex, x, targetSqlType);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		statement.setObject(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public boolean execute() throws SQLException {
		return execute(statement::execute);
	}

	@Override
	public void addBatch() throws SQLException {
		statement.addBatch();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		statement.setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		statement.setRef(parameterIndex, x);
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		statement.setBlob(parameterIndex, x);
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		statement.setClob(parameterIndex, x);
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		statement.setArray(parameterIndex, x);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return statement.getMetaData();
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		statement.setDate(parameterIndex, x, cal);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		statement.setTime(parameterIndex, x, cal);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		statement.setTimestamp(parameterIndex, x, cal);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		statement.setNull(parameterIndex, sqlType, typeName);
		parameters.remove(parameterIndex);
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		statement.setURL(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return statement.getParameterMetaData();
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		statement.setRowId(parameterIndex, x);
		parameters.put(parameterIndex, String.valueOf(x));
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		statement.setNString(parameterIndex, value);
		parameters.put(parameterIndex, value);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		statement.setNCharacterStream(parameterIndex, value, length);
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		statement.setNClob(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		statement.setClob(parameterIndex, reader, length);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		statement.setBlob(parameterIndex, inputStream, length);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		statement.setNClob(parameterIndex, reader, length);
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		statement.setSQLXML(parameterIndex, xmlObject);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		statement.setObject(parameterIndex, targetSqlType, scaleOrLength);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		statement.setAsciiStream(parameterIndex, x, length);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		statement.setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		statement.setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		statement.setAsciiStream(parameterIndex, x);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		statement.setBinaryStream(parameterIndex, x);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		statement.setCharacterStream(parameterIndex, reader);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		statement.setNCharacterStream(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		statement.setClob(parameterIndex, reader);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		statement.setBlob(parameterIndex, inputStream);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		statement.setNClob(parameterIndex, reader);
	}

}
