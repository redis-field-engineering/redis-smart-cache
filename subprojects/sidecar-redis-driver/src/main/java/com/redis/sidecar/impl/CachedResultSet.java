package com.redis.sidecar.impl;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.redis.sidecar.SidecarStatement;

public class CachedResultSet implements ResultSet {

	private final SidecarStatement statement;
	private final DataInputStream input;
	private final CachedResultSetMetaData metaData;
	private final Object[] currentRow;
	private final Object[] nextRow;
	private final HashMap<String, Integer> columnNames;
	private volatile boolean wasNull;
	private volatile int currentPos;
	private volatile int nextPos;
	private volatile boolean closed;

	public CachedResultSet(SidecarStatement statement, DataInputStream input) throws SQLException {
		this.statement = statement;
		this.wasNull = false;
		this.currentPos = 0;
		this.nextPos = 0;
		this.closed = false;
		this.input = input;
		try {
			this.metaData = new CachedResultSetMetaData(readColumns(input));
			this.currentRow = new Object[metaData.getColumns().length];
			this.nextRow = new Object[metaData.getColumns().length];
			this.columnNames = new HashMap<>();
			for (int index = 0; index < metaData.getColumns().length; index++) {
				Column column = metaData.getColumns()[index];
				this.columnNames.put(column.getLabel(), index + 1);
			}
			readNext();
		} catch (IOException e) {
			try {
				close();
			} catch (Exception ex) {
				// Close quietly
			}
			throw new SQLException("Cannot read the cache for statement " + statement, e);
		}
	}

	private Column[] readColumns(DataInputStream input) throws IOException {
		int columnCount = input.readInt();
		Column[] columns = new Column[columnCount];
		for (int index = 0; index < columnCount; index++) {
			columns[index] = new Column(input);
		}
		return columns;
	}

	private void readNext() throws SQLException {
		try {
			nextPos = input.readInt();
			if (nextPos != currentPos + 1) {
				throw new SQLException("Expects pos " + (currentPos + 1) + ", but got: " + nextPos);
			}
		} catch (EOFException e) {
			nextPos = 0;
			return;
		} catch (IOException e) {
			throw new SQLException(e);
		}
		for (int index = 0; index < metaData.getColumns().length; index++) {
			Column column = metaData.getColumns()[index];
			try {
				nextRow[index] = readRow(column.getType(), input);
			} catch (IOException e) {
				throw new SQLException("Cannot extract column " + index + " - pos " + nextPos, e);
			}
		}
	}

	private static Object readRow(int type, DataInputStream input) throws IOException {
		boolean wasNull = !input.readBoolean();
		if (wasNull)
			return null;
		switch (type) {
		case Types.BIT:
		case Types.BOOLEAN:
			return input.readBoolean();
		case Types.TINYINT:
			return input.readByte();
		case Types.SMALLINT:
			return input.readShort();
		case Types.INTEGER:
			return input.readInt();
		case Types.BIGINT:
			return input.readLong();
		case Types.FLOAT:
		case Types.REAL:
			return input.readFloat();
		case Types.DOUBLE:
		case Types.NUMERIC:
		case Types.DECIMAL:
			return input.readDouble();
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGNVARCHAR:
			return input.readUTF();
		case Types.DATE:
			return new java.sql.Date(input.readLong());
		case Types.TIME:
		case Types.TIME_WITH_TIMEZONE:
			return new java.sql.Time(input.readLong());
		case Types.TIMESTAMP:
		case Types.TIMESTAMP_WITH_TIMEZONE:
			return new java.sql.Timestamp(input.readLong());
		case Types.ROWID:
			return input.readUTF();
		case Types.CLOB:
			return input.readUTF();
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
		case Types.NULL:
		case Types.OTHER:
		case Types.JAVA_OBJECT:
		case Types.DISTINCT:
		case Types.STRUCT:
		case Types.ARRAY:
		case Types.BLOB:
		case Types.REF:
		case Types.DATALINK:
		case Types.NCLOB:
		case Types.SQLXML:
		case Types.REF_CURSOR:
		default:
			throw new IOException("Column type no supported: " + type);
		}
	}

	@Override
	public boolean next() throws SQLException {
		currentPos = nextPos;
		if (currentPos == 0) {
			return false;
		}
		System.arraycopy(nextRow, 0, currentRow, 0, nextRow.length);
		readNext();
		return true;
	}

	@Override
	public void close() throws SQLException {
		try {
			input.close();
			closed = true;
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public boolean wasNull() throws SQLException {
		return wasNull;
	}

	private Object checkColumn(final int columnIndex) throws SQLException {
		if (columnIndex == 0 || columnIndex > currentRow.length)
			throw new SQLException("Column out of bounds");
		final Object val = currentRow[columnIndex - 1];
		wasNull = val == null;
		return val;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		return val == null ? null : val.toString();
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return false;
		if (val instanceof Boolean)
			return (Boolean) val;
		if (val instanceof Number)
			return ((Number) val).intValue() == 0;
		return Boolean.parseBoolean(val.toString());
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return 0;
		if (val instanceof Byte)
			return (Byte) val;
		if (val instanceof Number)
			return ((Number) val).byteValue();
		return Byte.parseByte(val.toString());
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return 0;
		if (val instanceof Short)
			return (Short) val;
		if (val instanceof Number)
			return ((Number) val).shortValue();
		return Short.parseShort(val.toString());
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return 0;
		if (val instanceof Integer)
			return (Integer) val;
		if (val instanceof Number)
			return ((Number) val).intValue();
		return Integer.parseInt(val.toString());
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return 0;
		if (val instanceof Long)
			return (Long) val;
		if (val instanceof Number)
			return ((Number) val).longValue();
		return Long.parseLong(val.toString());
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return 0;
		if (val instanceof Float)
			return (Float) val;
		if (val instanceof Number)
			return ((Number) val).floatValue();
		return Float.parseFloat(val.toString());
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return 0;
		if (val instanceof Double)
			return (Double) val;
		if (val instanceof Number)
			return ((Number) val).doubleValue();
		return Double.parseDouble(val.toString());
	}

	@Override
	@Deprecated
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		return getBigDecimal(columnIndex);
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return null;
		if (val instanceof byte[])
			return (byte[]) val;
		return new byte[0];
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return null;
		if (val instanceof Date)
			return (Date) val;
		if (val instanceof Timestamp)
			return new Date(((Timestamp) val).getTime());
		if (val instanceof Number)
			return new Date(((Number) val).longValue());
		try {
			return new Date(DateFormat.getDateInstance().parse(val.toString()).getTime());
		} catch (ParseException e) {
			throw new SQLException("Unexpected Date type (" + val.getClass() + ") on column " + columnIndex, e);
		}
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return null;
		if (val instanceof Time)
			return (Time) val;
		if (val instanceof Number)
			return new Time(((Number) val).longValue());
		try {
			return new Time(DateFormat.getTimeInstance().parse(val.toString()).getTime());
		} catch (ParseException e) {
			throw new SQLException("Unexpected Time type (" + val.getClass() + ") on column " + columnIndex, e);
		}
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return null;
		if (val instanceof Timestamp)
			return (Timestamp) val;
		if (val instanceof Number)
			return new Timestamp(((Number) val).longValue());
		try {
			return new Timestamp(DateFormat.getDateTimeInstance().parse(val.toString()).getTime());
		} catch (ParseException e) {
			throw new SQLException("Unexpected Timestamp type (" + val.getClass() + ") on column " + columnIndex, e);
		}
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		checkColumn(columnIndex);
		return null;
	}

	@Override
	@Deprecated
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		checkColumn(columnIndex);
		return null;
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		checkColumn(columnIndex);
		return null;
	}

	private int checkColumn(final String label) throws SQLException {
		final Integer colIdx = columnNames.get(label);
		if (colIdx == null)
			throw new SQLException("Column not found: " + label);
		return colIdx;
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return getString(checkColumn(columnLabel));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(checkColumn(columnLabel));
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return getByte(checkColumn(columnLabel));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return getShort(checkColumn(columnLabel));
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return getInt(checkColumn(columnLabel));
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return getLong(checkColumn(columnLabel));
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return getFloat(checkColumn(columnLabel));
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return getDouble(checkColumn(columnLabel));
	}

	@Override
	@Deprecated
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return getBigDecimal(checkColumn(columnLabel));
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		return getBytes(checkColumn(columnLabel));
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return getDate(checkColumn(columnLabel));
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return getTime(checkColumn(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(checkColumn(columnLabel));
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return getAsciiStream(checkColumn(columnLabel));
	}

	@Override
	@Deprecated
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return getUnicodeStream(checkColumn(columnLabel));
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return getBinaryStream(checkColumn(columnLabel));
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public String getCursorName() throws SQLException {
		return null;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metaData;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return null;
		return val;
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return getObject(checkColumn(columnLabel));
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		final Integer colIdx = columnNames.get(columnLabel);
		return colIdx == null ? 0 : colIdx;
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		checkColumn(columnIndex);
		return null;
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		return getCharacterStream(checkColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return null;
		if (val instanceof BigDecimal)
			return (BigDecimal) val;
		if (val instanceof Number)
			return new BigDecimal(((Number) val).doubleValue());
		return new BigDecimal(Double.parseDouble(val.toString()));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(checkColumn(columnLabel));
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return currentPos == 0;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return currentPos == 0 && nextPos == 0;
	}

	@Override
	public boolean isFirst() throws SQLException {
		return currentPos == 1;
	}

	@Override
	public boolean isLast() throws SQLException {
		return nextPos == 0 && currentPos != 0;
	}

	@Override
	public void beforeFirst() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void afterLast() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean first() throws SQLException {
		return false;
	}

	@Override
	public boolean last() throws SQLException {
		return false;
	}

	@Override
	public int getRow() throws SQLException {
		return currentPos;
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		return false;
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		return false;
	}

	@Override
	public boolean previous() throws SQLException {
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return statement.getFetchDirection();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {

	}

	@Override
	public int getFetchSize() throws SQLException {
		return statement.getFetchSize();
	}

	@Override
	public int getType() throws SQLException {
		return statement.getResultSetType();
	}

	@Override
	public int getConcurrency() throws SQLException {
		return statement.getResultSetConcurrency();
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		return false;
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement getStatement() throws SQLException {
		return statement;
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		checkColumn(columnIndex);
		return null;
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		checkColumn(columnIndex);
		return null;
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		final Object val = checkColumn(columnIndex);
		if (val == null)
			return null;
		return new ClobString(val.toString());
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		checkColumn(columnIndex);
		return null;
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		return getObject(checkColumn(columnLabel));
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		return getRef(checkColumn(columnLabel));
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		return getBlob(checkColumn(columnLabel));
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		return getClob(checkColumn(columnLabel));
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		return getArray(checkColumn(columnLabel));
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		return getDate(columnIndex);
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		return getDate(columnLabel);
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		return getTime(columnIndex);
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		return getTime(columnLabel);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		return getTimestamp(columnIndex);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		return getTimestamp(columnLabel);
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		Object val = checkColumn(columnIndex);
		if (val == null)
			return null;
		if (val instanceof URL)
			return (URL) val;
		try {
			return new URL(val.toString());
		} catch (MalformedURLException e) {
			throw new SQLException("Cannot extract url: " + val, e);
		}
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		return getURL(checkColumn(columnLabel));
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		Object val = checkColumn(columnIndex);
		if (val == null)
			return null;
		if (val instanceof RowId)
			return (RowId) val;
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		return getRowId(checkColumn(columnLabel));
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getHoldability() throws SQLException {
		return statement.getResultSetHoldability();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		checkColumn(columnIndex);
		return null;
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		return getNClob(checkColumn(columnLabel));
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		checkColumn(columnIndex);
		return null;
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		return getSQLXML(checkColumn(columnLabel));
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		return getString(columnIndex);
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		return getString(columnLabel);
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		checkColumn(columnIndex);
		return null;
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		return getNCharacterStream(checkColumn(columnLabel));
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return getObject(checkColumn(columnLabel), type);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
}
