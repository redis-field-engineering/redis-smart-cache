package com.redis.sidecar.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class SidecarCallableStatement extends SidecarPreparedStatement implements CallableStatement {

	private final SortedMap<String, String> parameters = new TreeMap<>();
	private final CallableStatement statement;

	public SidecarCallableStatement(SidecarConnection connection, CallableStatement statement, String sql) {
		super(connection, statement, sql);
		this.statement = statement;
	}

	@Override
	protected void appendParameters(StringBuilder stringBuilder) {
		super.appendParameters(stringBuilder);
		parameters.forEach((name, value) -> stringBuilder.append('•').append(name).append(value));
	}

	@Override
	public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
		statement.registerOutParameter(parameterIndex, sqlType);
	}

	@Override
	public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
		statement.registerOutParameter(parameterIndex, sqlType, scale);
	}

	@Override
	public boolean wasNull() throws SQLException {
		return statement.wasNull();
	}

	@Override
	public String getString(int parameterIndex) throws SQLException {
		return statement.getString(parameterIndex);
	}

	@Override
	public boolean getBoolean(int parameterIndex) throws SQLException {
		return statement.getBoolean(parameterIndex);
	}

	@Override
	public byte getByte(int parameterIndex) throws SQLException {
		return statement.getByte(parameterIndex);
	}

	@Override
	public short getShort(int parameterIndex) throws SQLException {
		return statement.getShort(parameterIndex);
	}

	@Override
	public int getInt(int parameterIndex) throws SQLException {
		return statement.getInt(parameterIndex);
	}

	@Override
	public long getLong(int parameterIndex) throws SQLException {
		return statement.getLong(parameterIndex);
	}

	@Override
	public float getFloat(int parameterIndex) throws SQLException {
		return statement.getFloat(parameterIndex);
	}

	@Override
	public double getDouble(int parameterIndex) throws SQLException {
		return statement.getDouble(parameterIndex);
	}

	@SuppressWarnings("deprecation")
	@Override
	public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
		return statement.getBigDecimal(parameterIndex, scale);
	}

	@Override
	public byte[] getBytes(int parameterIndex) throws SQLException {
		return statement.getBytes(parameterIndex);
	}

	@Override
	public Date getDate(int parameterIndex) throws SQLException {
		return statement.getDate(parameterIndex);
	}

	@Override
	public Time getTime(int parameterIndex) throws SQLException {
		return statement.getTime(parameterIndex);
	}

	@Override
	public Timestamp getTimestamp(int parameterIndex) throws SQLException {
		return statement.getTimestamp(parameterIndex);
	}

	@Override
	public Object getObject(int parameterIndex) throws SQLException {
		return statement.getObject(parameterIndex);
	}

	@Override
	public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
		return statement.getBigDecimal(parameterIndex);
	}

	@Override
	public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
		return statement.getObject(parameterIndex, map);
	}

	@Override
	public Ref getRef(int parameterIndex) throws SQLException {
		return statement.getRef(parameterIndex);
	}

	@Override
	public Blob getBlob(int parameterIndex) throws SQLException {
		return statement.getBlob(parameterIndex);
	}

	@Override
	public Clob getClob(int parameterIndex) throws SQLException {
		return statement.getClob(parameterIndex);
	}

	@Override
	public Array getArray(int parameterIndex) throws SQLException {
		return statement.getArray(parameterIndex);
	}

	@Override
	public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
		return statement.getDate(parameterIndex, cal);
	}

	@Override
	public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
		return statement.getTime(parameterIndex, cal);
	}

	@Override
	public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
		return statement.getTimestamp(parameterIndex, cal);
	}

	@Override
	public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
		statement.registerOutParameter(parameterIndex, sqlType, typeName);
	}

	@Override
	public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
		statement.registerOutParameter(sqlType, sqlType);
	}

	@Override
	public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
		statement.registerOutParameter(scale, sqlType, scale);
	}

	@Override
	public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
		statement.registerOutParameter(sqlType, sqlType, typeName);
	}

	@Override
	public URL getURL(int parameterIndex) throws SQLException {
		return statement.getURL(parameterIndex);
	}

	@Override
	public void setURL(String parameterName, URL val) throws SQLException {
		statement.setURL(parameterName, val);
		parameters.put(parameterName, String.valueOf(val));
	}

	@Override
	public void setNull(String parameterName, int sqlType) throws SQLException {
		statement.setNull(parameterName, sqlType);
		parameters.remove(parameterName);
	}

	@Override
	public void setBoolean(String parameterName, boolean x) throws SQLException {
		statement.setBoolean(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setByte(String parameterName, byte x) throws SQLException {
		statement.setByte(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setShort(String parameterName, short x) throws SQLException {
		statement.setShort(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setInt(String parameterName, int x) throws SQLException {
		statement.setInt(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setLong(String parameterName, long x) throws SQLException {
		statement.setLong(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setFloat(String parameterName, float x) throws SQLException {
		statement.setFloat(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setDouble(String parameterName, double x) throws SQLException {
		statement.setDouble(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
		statement.setBigDecimal(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setString(String parameterName, String x) throws SQLException {
		statement.setString(parameterName, x);
		parameters.put(parameterName, x);
	}

	@Override
	public void setBytes(String parameterName, byte[] x) throws SQLException {
		statement.setBytes(parameterName, x);
		parameters.put(parameterName, new String(x));
	}

	@Override
	public void setDate(String parameterName, Date x) throws SQLException {
		statement.setDate(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setTime(String parameterName, Time x) throws SQLException {
		statement.setTime(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
		statement.setTimestamp(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
		statement.setAsciiStream(length, x, length);
	}

	@Override
	public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
		statement.setBinaryStream(parameterName, x, length);
	}

	@Override
	public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
		statement.setObject(parameterName, x, targetSqlType, scale);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
		statement.setObject(parameterName, x, targetSqlType);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setObject(String parameterName, Object x) throws SQLException {
		statement.setObject(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
		statement.setCharacterStream(parameterName, reader, length);
	}

	@Override
	public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
		statement.setDate(parameterName, x, cal);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
		statement.setTime(parameterName, x, cal);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
		statement.setTimestamp(parameterName, x, cal);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
		statement.setNull(parameterName, sqlType, typeName);
		parameters.remove(parameterName);
	}

	@Override
	public String getString(String parameterName) throws SQLException {
		return statement.getString(parameterName);
	}

	@Override
	public boolean getBoolean(String parameterName) throws SQLException {
		return statement.getBoolean(parameterName);
	}

	@Override
	public byte getByte(String parameterName) throws SQLException {
		return statement.getByte(parameterName);
	}

	@Override
	public short getShort(String parameterName) throws SQLException {
		return statement.getShort(parameterName);
	}

	@Override
	public int getInt(String parameterName) throws SQLException {
		return statement.getInt(parameterName);
	}

	@Override
	public long getLong(String parameterName) throws SQLException {
		return statement.getLong(parameterName);
	}

	@Override
	public float getFloat(String parameterName) throws SQLException {
		return statement.getFloat(parameterName);
	}

	@Override
	public double getDouble(String parameterName) throws SQLException {
		return statement.getDouble(parameterName);
	}

	@Override
	public byte[] getBytes(String parameterName) throws SQLException {
		return statement.getBytes(parameterName);
	}

	@Override
	public Date getDate(String parameterName) throws SQLException {
		return statement.getDate(parameterName);
	}

	@Override
	public Time getTime(String parameterName) throws SQLException {
		return statement.getTime(parameterName);
	}

	@Override
	public Timestamp getTimestamp(String parameterName) throws SQLException {
		return statement.getTimestamp(parameterName);
	}

	@Override
	public Object getObject(String parameterName) throws SQLException {
		return statement.getObject(parameterName);
	}

	@Override
	public BigDecimal getBigDecimal(String parameterName) throws SQLException {
		return statement.getBigDecimal(parameterName);
	}

	@Override
	public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
		return statement.getObject(parameterName);
	}

	@Override
	public Ref getRef(String parameterName) throws SQLException {
		return statement.getRef(parameterName);
	}

	@Override
	public Blob getBlob(String parameterName) throws SQLException {
		return statement.getBlob(parameterName);
	}

	@Override
	public Clob getClob(String parameterName) throws SQLException {
		return statement.getClob(parameterName);
	}

	@Override
	public Array getArray(String parameterName) throws SQLException {
		return statement.getArray(parameterName);
	}

	@Override
	public Date getDate(String parameterName, Calendar cal) throws SQLException {
		return statement.getDate(parameterName);
	}

	@Override
	public Time getTime(String parameterName, Calendar cal) throws SQLException {
		return statement.getTime(parameterName);
	}

	@Override
	public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
		return statement.getTimestamp(parameterName);
	}

	@Override
	public URL getURL(String parameterName) throws SQLException {
		return statement.getURL(parameterName);
	}

	@Override
	public RowId getRowId(int parameterIndex) throws SQLException {
		return statement.getRowId(parameterIndex);
	}

	@Override
	public RowId getRowId(String parameterName) throws SQLException {
		return statement.getRowId(parameterName);
	}

	@Override
	public void setRowId(String parameterName, RowId x) throws SQLException {
		statement.setRowId(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setNString(String parameterName, String value) throws SQLException {
		statement.setNString(parameterName, value);
		parameters.put(parameterName, value);
	}

	@Override
	public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
		statement.setNCharacterStream(parameterName, value, length);
	}

	@Override
	public void setNClob(String parameterName, NClob value) throws SQLException {
		statement.setNClob(parameterName, value);
	}

	@Override
	public void setClob(String parameterName, Reader reader, long length) throws SQLException {
		statement.setClob(parameterName, reader, length);
	}

	@Override
	public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
		statement.setBlob(parameterName, inputStream, length);
	}

	@Override
	public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
		statement.setNClob(parameterName, reader, length);
	}

	@Override
	public NClob getNClob(int parameterIndex) throws SQLException {
		return statement.getNClob(parameterIndex);
	}

	@Override
	public NClob getNClob(String parameterName) throws SQLException {
		return statement.getNClob(parameterName);
	}

	@Override
	public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
		statement.setSQLXML(parameterName, xmlObject);
		parameters.put(parameterName, String.valueOf(xmlObject));
	}

	@Override
	public SQLXML getSQLXML(int parameterIndex) throws SQLException {
		return statement.getSQLXML(parameterIndex);
	}

	@Override
	public SQLXML getSQLXML(String parameterName) throws SQLException {
		return statement.getSQLXML(parameterName);
	}

	@Override
	public String getNString(int parameterIndex) throws SQLException {
		return statement.getNString(parameterIndex);
	}

	@Override
	public String getNString(String parameterName) throws SQLException {
		return statement.getNString(parameterName);
	}

	@Override
	public Reader getNCharacterStream(int parameterIndex) throws SQLException {
		return statement.getNCharacterStream(parameterIndex);
	}

	@Override
	public Reader getNCharacterStream(String parameterName) throws SQLException {
		return statement.getNCharacterStream(parameterName);
	}

	@Override
	public Reader getCharacterStream(int parameterIndex) throws SQLException {
		return statement.getCharacterStream(parameterIndex);
	}

	@Override
	public Reader getCharacterStream(String parameterName) throws SQLException {
		return statement.getCharacterStream(parameterName);
	}

	@Override
	public void setBlob(String parameterName, Blob x) throws SQLException {
		statement.setBlob(parameterName, x);
	}

	@Override
	public void setClob(String parameterName, Clob x) throws SQLException {
		statement.setClob(parameterName, x);
	}

	@Override
	public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
		statement.setAsciiStream(parameterName, x, length);

	}

	@Override
	public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
		statement.setBinaryStream(parameterName, x, length);
	}

	@Override
	public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
		statement.setCharacterStream(parameterName, reader, length);
	}

	@Override
	public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
		statement.setAsciiStream(parameterName, x);
	}

	@Override
	public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
		statement.setBinaryStream(parameterName, x);
	}

	@Override
	public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
		statement.setCharacterStream(parameterName, reader);
	}

	@Override
	public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
		statement.setNCharacterStream(parameterName, value);
	}

	@Override
	public void setClob(String parameterName, Reader reader) throws SQLException {
		statement.setClob(parameterName, reader);
	}

	@Override
	public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
		statement.setBlob(parameterName, inputStream);
	}

	@Override
	public void setNClob(String parameterName, Reader reader) throws SQLException {
		statement.setNClob(parameterName, reader);
	}

	@Override
	public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
		return statement.getObject(parameterIndex, type);
	}

	@Override
	public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
		return statement.getObject(parameterName, type);
	}

}
