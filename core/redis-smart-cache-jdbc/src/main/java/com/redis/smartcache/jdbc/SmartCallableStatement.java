package com.redis.smartcache.jdbc;

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
import java.util.Map.Entry;

import com.redis.smartcache.core.Query;

import java.util.SortedMap;
import java.util.TreeMap;

public class SmartCallableStatement extends SmartPreparedStatement implements CallableStatement {

	private final SortedMap<String, String> parameters = new TreeMap<>();

	public SmartCallableStatement(SmartConnection connection, CallableStatement statement, String sql) {
		super(connection, statement, sql);
	}

	@Override
	protected String key(Query query) {
		StringBuilder builder = new StringBuilder(query.getKey());
		for (Entry<String, String> entry : parameters.entrySet()) {
			builder.append(PARAMETER_SEPARATOR).append(entry.getKey() + "=" + entry.getValue());
		}
		return builder.toString();
	}

	@Override
	public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
		((CallableStatement) statement).registerOutParameter(parameterIndex, sqlType);
	}

	@Override
	public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
		((CallableStatement) statement).registerOutParameter(parameterIndex, sqlType, scale);
	}

	@Override
	public boolean wasNull() throws SQLException {
		return ((CallableStatement) statement).wasNull();
	}

	@Override
	public String getString(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getString(parameterIndex);
	}

	@Override
	public boolean getBoolean(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getBoolean(parameterIndex);
	}

	@Override
	public byte getByte(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getByte(parameterIndex);
	}

	@Override
	public short getShort(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getShort(parameterIndex);
	}

	@Override
	public int getInt(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getInt(parameterIndex);
	}

	@Override
	public long getLong(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getLong(parameterIndex);
	}

	@Override
	public float getFloat(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getFloat(parameterIndex);
	}

	@Override
	public double getDouble(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getDouble(parameterIndex);
	}

	@SuppressWarnings("deprecation")
	@Override
	public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
		return ((CallableStatement) statement).getBigDecimal(parameterIndex, scale);
	}

	@Override
	public byte[] getBytes(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getBytes(parameterIndex);
	}

	@Override
	public Date getDate(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getDate(parameterIndex);
	}

	@Override
	public Time getTime(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getTime(parameterIndex);
	}

	@Override
	public Timestamp getTimestamp(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getTimestamp(parameterIndex);
	}

	@Override
	public Object getObject(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getObject(parameterIndex);
	}

	@Override
	public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getBigDecimal(parameterIndex);
	}

	@Override
	public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
		return ((CallableStatement) statement).getObject(parameterIndex, map);
	}

	@Override
	public Ref getRef(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getRef(parameterIndex);
	}

	@Override
	public Blob getBlob(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getBlob(parameterIndex);
	}

	@Override
	public Clob getClob(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getClob(parameterIndex);
	}

	@Override
	public Array getArray(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getArray(parameterIndex);
	}

	@Override
	public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
		return ((CallableStatement) statement).getDate(parameterIndex, cal);
	}

	@Override
	public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
		return ((CallableStatement) statement).getTime(parameterIndex, cal);
	}

	@Override
	public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
		return ((CallableStatement) statement).getTimestamp(parameterIndex, cal);
	}

	@Override
	public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
		((CallableStatement) statement).registerOutParameter(parameterIndex, sqlType, typeName);
	}

	@Override
	public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
		((CallableStatement) statement).registerOutParameter(sqlType, sqlType);
	}

	@Override
	public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
		((CallableStatement) statement).registerOutParameter(scale, sqlType, scale);
	}

	@Override
	public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
		((CallableStatement) statement).registerOutParameter(sqlType, sqlType, typeName);
	}

	@Override
	public URL getURL(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getURL(parameterIndex);
	}

	@Override
	public void setURL(String parameterName, URL val) throws SQLException {
		((CallableStatement) statement).setURL(parameterName, val);
		parameters.put(parameterName, String.valueOf(val));
	}

	@Override
	public void setNull(String parameterName, int sqlType) throws SQLException {
		((CallableStatement) statement).setNull(parameterName, sqlType);
		parameters.remove(parameterName);
	}

	@Override
	public void setBoolean(String parameterName, boolean x) throws SQLException {
		((CallableStatement) statement).setBoolean(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setByte(String parameterName, byte x) throws SQLException {
		((CallableStatement) statement).setByte(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setShort(String parameterName, short x) throws SQLException {
		((CallableStatement) statement).setShort(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setInt(String parameterName, int x) throws SQLException {
		((CallableStatement) statement).setInt(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setLong(String parameterName, long x) throws SQLException {
		((CallableStatement) statement).setLong(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setFloat(String parameterName, float x) throws SQLException {
		((CallableStatement) statement).setFloat(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setDouble(String parameterName, double x) throws SQLException {
		((CallableStatement) statement).setDouble(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
		((CallableStatement) statement).setBigDecimal(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setString(String parameterName, String x) throws SQLException {
		((CallableStatement) statement).setString(parameterName, x);
		parameters.put(parameterName, x);
	}

	@Override
	public void setBytes(String parameterName, byte[] x) throws SQLException {
		((CallableStatement) statement).setBytes(parameterName, x);
		parameters.put(parameterName, new String(x));
	}

	@Override
	public void setDate(String parameterName, Date x) throws SQLException {
		((CallableStatement) statement).setDate(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setTime(String parameterName, Time x) throws SQLException {
		((CallableStatement) statement).setTime(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
		((CallableStatement) statement).setTimestamp(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
		((CallableStatement) statement).setAsciiStream(length, x, length);
	}

	@Override
	public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
		((CallableStatement) statement).setBinaryStream(parameterName, x, length);
	}

	@Override
	public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
		((CallableStatement) statement).setObject(parameterName, x, targetSqlType, scale);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
		((CallableStatement) statement).setObject(parameterName, x, targetSqlType);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setObject(String parameterName, Object x) throws SQLException {
		((CallableStatement) statement).setObject(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
		((CallableStatement) statement).setCharacterStream(parameterName, reader, length);
	}

	@Override
	public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
		((CallableStatement) statement).setDate(parameterName, x, cal);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
		((CallableStatement) statement).setTime(parameterName, x, cal);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
		((CallableStatement) statement).setTimestamp(parameterName, x, cal);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
		((CallableStatement) statement).setNull(parameterName, sqlType, typeName);
		parameters.remove(parameterName);
	}

	@Override
	public String getString(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getString(parameterName);
	}

	@Override
	public boolean getBoolean(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getBoolean(parameterName);
	}

	@Override
	public byte getByte(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getByte(parameterName);
	}

	@Override
	public short getShort(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getShort(parameterName);
	}

	@Override
	public int getInt(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getInt(parameterName);
	}

	@Override
	public long getLong(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getLong(parameterName);
	}

	@Override
	public float getFloat(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getFloat(parameterName);
	}

	@Override
	public double getDouble(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getDouble(parameterName);
	}

	@Override
	public byte[] getBytes(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getBytes(parameterName);
	}

	@Override
	public Date getDate(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getDate(parameterName);
	}

	@Override
	public Time getTime(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getTime(parameterName);
	}

	@Override
	public Timestamp getTimestamp(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getTimestamp(parameterName);
	}

	@Override
	public Object getObject(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getObject(parameterName);
	}

	@Override
	public BigDecimal getBigDecimal(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getBigDecimal(parameterName);
	}

	@Override
	public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
		return ((CallableStatement) statement).getObject(parameterName);
	}

	@Override
	public Ref getRef(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getRef(parameterName);
	}

	@Override
	public Blob getBlob(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getBlob(parameterName);
	}

	@Override
	public Clob getClob(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getClob(parameterName);
	}

	@Override
	public Array getArray(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getArray(parameterName);
	}

	@Override
	public Date getDate(String parameterName, Calendar cal) throws SQLException {
		return ((CallableStatement) statement).getDate(parameterName);
	}

	@Override
	public Time getTime(String parameterName, Calendar cal) throws SQLException {
		return ((CallableStatement) statement).getTime(parameterName);
	}

	@Override
	public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
		return ((CallableStatement) statement).getTimestamp(parameterName);
	}

	@Override
	public URL getURL(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getURL(parameterName);
	}

	@Override
	public RowId getRowId(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getRowId(parameterIndex);
	}

	@Override
	public RowId getRowId(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getRowId(parameterName);
	}

	@Override
	public void setRowId(String parameterName, RowId x) throws SQLException {
		((CallableStatement) statement).setRowId(parameterName, x);
		parameters.put(parameterName, String.valueOf(x));
	}

	@Override
	public void setNString(String parameterName, String value) throws SQLException {
		((CallableStatement) statement).setNString(parameterName, value);
		parameters.put(parameterName, value);
	}

	@Override
	public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
		((CallableStatement) statement).setNCharacterStream(parameterName, value, length);
	}

	@Override
	public void setNClob(String parameterName, NClob value) throws SQLException {
		((CallableStatement) statement).setNClob(parameterName, value);
	}

	@Override
	public void setClob(String parameterName, Reader reader, long length) throws SQLException {
		((CallableStatement) statement).setClob(parameterName, reader, length);
	}

	@Override
	public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
		((CallableStatement) statement).setBlob(parameterName, inputStream, length);
	}

	@Override
	public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
		((CallableStatement) statement).setNClob(parameterName, reader, length);
	}

	@Override
	public NClob getNClob(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getNClob(parameterIndex);
	}

	@Override
	public NClob getNClob(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getNClob(parameterName);
	}

	@Override
	public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
		((CallableStatement) statement).setSQLXML(parameterName, xmlObject);
		parameters.put(parameterName, String.valueOf(xmlObject));
	}

	@Override
	public SQLXML getSQLXML(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getSQLXML(parameterIndex);
	}

	@Override
	public SQLXML getSQLXML(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getSQLXML(parameterName);
	}

	@Override
	public String getNString(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getNString(parameterIndex);
	}

	@Override
	public String getNString(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getNString(parameterName);
	}

	@Override
	public Reader getNCharacterStream(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getNCharacterStream(parameterIndex);
	}

	@Override
	public Reader getNCharacterStream(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getNCharacterStream(parameterName);
	}

	@Override
	public Reader getCharacterStream(int parameterIndex) throws SQLException {
		return ((CallableStatement) statement).getCharacterStream(parameterIndex);
	}

	@Override
	public Reader getCharacterStream(String parameterName) throws SQLException {
		return ((CallableStatement) statement).getCharacterStream(parameterName);
	}

	@Override
	public void setBlob(String parameterName, Blob x) throws SQLException {
		((CallableStatement) statement).setBlob(parameterName, x);
	}

	@Override
	public void setClob(String parameterName, Clob x) throws SQLException {
		((CallableStatement) statement).setClob(parameterName, x);
	}

	@Override
	public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
		((CallableStatement) statement).setAsciiStream(parameterName, x, length);

	}

	@Override
	public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
		((CallableStatement) statement).setBinaryStream(parameterName, x, length);
	}

	@Override
	public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
		((CallableStatement) statement).setCharacterStream(parameterName, reader, length);
	}

	@Override
	public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
		((CallableStatement) statement).setAsciiStream(parameterName, x);
	}

	@Override
	public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
		((CallableStatement) statement).setBinaryStream(parameterName, x);
	}

	@Override
	public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
		((CallableStatement) statement).setCharacterStream(parameterName, reader);
	}

	@Override
	public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
		((CallableStatement) statement).setNCharacterStream(parameterName, value);
	}

	@Override
	public void setClob(String parameterName, Reader reader) throws SQLException {
		((CallableStatement) statement).setClob(parameterName, reader);
	}

	@Override
	public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
		((CallableStatement) statement).setBlob(parameterName, inputStream);
	}

	@Override
	public void setNClob(String parameterName, Reader reader) throws SQLException {
		((CallableStatement) statement).setNClob(parameterName, reader);
	}

	@Override
	public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
		return ((CallableStatement) statement).getObject(parameterIndex, type);
	}

	@Override
	public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
		return ((CallableStatement) statement).getObject(parameterName, type);
	}

}
