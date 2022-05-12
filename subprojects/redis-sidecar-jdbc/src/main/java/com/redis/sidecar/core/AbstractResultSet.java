package com.redis.sidecar.core;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringBufferInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
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
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.sql.rowset.serial.SQLInputImpl;

import com.redis.sidecar.jdbc.SidecarResultSetMetaData;

import io.lettuce.core.internal.LettuceAssert;

@SuppressWarnings("deprecation")
public abstract class AbstractResultSet implements ResultSet {

	private static final String MESSAGE_DATA_TYPE_MISMATCH = "Data type mismatch";
	private static final String MESSAGE_FAILED_GET = "get%s failed on value '%s' in column %s";
	private static final String MESSAGE_FAILED_GET_CONVERSION = MESSAGE_FAILED_GET + "; no conversion available";

	/**
	 * A <code>boolean</code> indicating whether the last value returned was an SQL
	 * <code>NULL</code>.
	 * 
	 */
	private boolean lastValueNull;

	/**
	 * The <code>ResultSetMetaData</code> object that contains information about the
	 * columns in the <code>ResultSet</code> object.
	 */
	private final SidecarResultSetMetaData metaData;

	/**
	 * A <code>SQLWarning</code> which logs on the warnings
	 */
	private SQLWarning sqlwarn = new SQLWarning();

	private Map<String, Class<?>> typeMap;

	protected AbstractResultSet(SidecarResultSetMetaData metaData) {
		LettuceAssert.notNull(metaData, "MetaData cannot be null");
		this.metaData = metaData;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public boolean wasNull() throws SQLException {
		return lastValueNull;
	}

	private Object getColumnObject(int columnIndex) {
		return getCurrentRow().get(columnIndex - 1);
	}

	/**
	 * Returns the column number of the column with the given name in this
	 * <code>CachedRowSetImpl</code> object. This method throws an
	 * <code>SQLException</code> if the given name is not the name of one of the
	 * columns in this rowset.
	 *
	 * @param name a <code>String</code> object that is the name of a column in this
	 *             <code>CachedRowSetImpl</code> object
	 * @throws SQLException if the given name does not match the name of one of the
	 *                      columns in this rowset
	 */
	private int getColumnIndex(String name) throws SQLException {
		Integer columnIndex = metaData.getColumnIndex(name);
		if (columnIndex == null) {
			throw new SQLException("Invalid column name");
		}
		return columnIndex;
	}

	protected abstract List<Object> getCurrentRow();

	/**
	 * Sets the field <code>lastValueNull</code> to the given <code>boolean</code>
	 * value.
	 *
	 * @param value <code>true</code> to indicate that the value of the last column
	 *              read was SQL <code>NULL</code>; <code>false</code> to indicate
	 *              that it was not
	 */
	private void setLastValueNull(boolean value) {
		lastValueNull = value;
	}

	private void checkIndex(int columnIndex) throws SQLException {
		if (columnIndex < 1 || columnIndex > metaData.getColumnCount()) {
			throw new SQLException("Invalid column index");
		}
	}

	/**
	 * Checks to see whether the cursor for this <code>CachedRowSetImpl</code>
	 * object is on a row in the rowset and throws an <code>SQLException</code> if
	 * it is not.
	 * <P>
	 * This method is called internally by <code>getXXX</code> methods, by
	 * <code>updateXXX</code> methods, and by methods that update, insert, or delete
	 * a row or that cancel a row update, insert, or delete.
	 *
	 * @throws SQLException if the cursor for this <code>CachedRowSetImpl</code>
	 *                      object is not on a valid row
	 */
	private void checkCursor() throws SQLException {
		if (isAfterLast() || isBeforeFirst()) {
			throw new SQLException("Invalid cursor position");
		}
	}

	/**
	 * Indicates whether the given SQL data type is a binary type.
	 *
	 * @param type one of the constants from <code>java.sql.Types</code>
	 * @return <code>true</code> if the given type is <code>BINARY</code>,'
	 *         <code>VARBINARY</code>, or <code>LONGVARBINARY</code>;
	 *         <code>false</code> otherwise
	 */
	private boolean isBinary(int type) {
		switch (type) {
		case java.sql.Types.BINARY:
		case java.sql.Types.VARBINARY:
		case java.sql.Types.LONGVARBINARY:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Indicates whether the given SQL data type is a string type.
	 *
	 * @param type one of the constants from <code>java.sql.Types</code>
	 * @return <code>true</code> if the given type is <code>CHAR</code>,'
	 *         <code>VARCHAR</code>, or <code>LONGVARCHAR</code>; <code>false</code>
	 *         otherwise
	 */
	private boolean isString(int type) {
		switch (type) {
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
		case java.sql.Types.LONGVARCHAR:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Indicates whether the given SQL data type is a numberic type.
	 *
	 * @param type one of the constants from <code>java.sql.Types</code>
	 * @return <code>true</code> if the given type is <code>NUMERIC</code>,'
	 *         <code>DECIMAL</code>, <code>BIT</code>, <code>TINYINT</code>,
	 *         <code>SMALLINT</code>, <code>INTEGER</code>, <code>BIGINT</code>,
	 *         <code>REAL</code>, <code>DOUBLE</code>, or <code>FLOAT</code>;
	 *         <code>false</code> otherwise
	 */
	private boolean isNumeric(int type) {
		switch (type) {
		case java.sql.Types.NUMERIC:
		case java.sql.Types.DECIMAL:
		case java.sql.Types.BIT:
		case java.sql.Types.TINYINT:
		case java.sql.Types.SMALLINT:
		case java.sql.Types.INTEGER:
		case java.sql.Types.BIGINT:
		case java.sql.Types.REAL:
		case java.sql.Types.DOUBLE:
		case java.sql.Types.FLOAT:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Converts the given <code>Object</code> in the Java programming language to
	 * the standard object mapping for the specified SQL target data type. The
	 * conversion must be to a string or temporal type, and there are also
	 * restrictions on the type to be converted.
	 * <P>
	 * <TABLE ALIGN="CENTER" BORDER CELLPADDING=10 BORDERCOLOR="#0000FF"
	 * <CAPTION ALIGN="CENTER"><B>Parameters and Return Values</B></CAPTION>
	 * <TR>
	 * <TD><B>Source SQL Type</B>
	 * <TD><B>Target SQL Type</B>
	 * <TD><B>Object Returned</B>
	 * </TR>
	 * <TR>
	 * <TD><code>TIMESTAMP</code>
	 * <TD><code>DATE</code>
	 * <TD><code>java.sql.Date</code>
	 * </TR>
	 * <TR>
	 * <TD><code>TIMESTAMP</code>
	 * <TD><code>TIME</code>
	 * <TD><code>java.sql.Time</code>
	 * </TR>
	 * <TR>
	 * <TD><code>TIME</code>
	 * <TD><code>TIMESTAMP</code>
	 * <TD><code>java.sql.Timestamp</code>
	 * </TR>
	 * <TR>
	 * <TD><code>DATE</code>, <code>TIME</code>, or <code>TIMESTAMP</code>
	 * <TD><code>CHAR</code>, <code>VARCHAR</code>, or <code>LONGVARCHAR</code>
	 * <TD><code>java.lang.String</code>
	 * </TR>
	 * </TABLE>
	 * <P>
	 * If the source type and target type are the same, the given object is simply
	 * returned.
	 *
	 * @param srcObj  the <code>Object</code> in the Java programming language that
	 *                is to be converted to the target type
	 * @param srcType the data type that is the standard mapping in SQL of the
	 *                object to be converted; must be one of the constants in
	 *                <code>java.sql.Types</code>
	 * @param trgType the SQL data type to which to convert the given object; must
	 *                be one of the following constants in
	 *                <code>java.sql.Types</code>: <code>DATE</code>,
	 *                <code>TIME</code>, <code>TIMESTAMP</code>, <code>CHAR</code>,
	 *                <code>VARCHAR</code>, or <code>LONGVARCHAR</code>
	 * @return an <code>Object</code> value.that is the standard object mapping for
	 *         the target SQL type
	 * @throws SQLException if the given target type is not one of the string or
	 *                      temporal types in <code>java.sql.Types</code>
	 */
	private Object convertTemporal(Object srcObj, int srcType, int trgType) throws SQLException {

		if (srcType == trgType) {
			return srcObj;
		}

		if (isNumeric(trgType) || (!isString(trgType) && !isTemporal(trgType))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		try {
			switch (trgType) {
			case java.sql.Types.DATE:
				if (srcType == java.sql.Types.TIMESTAMP) {
					return new java.sql.Date(((java.sql.Timestamp) srcObj).getTime());
				} else {
					throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
				}
			case java.sql.Types.TIMESTAMP:
				if (srcType == java.sql.Types.TIME) {
					return new Timestamp(((java.sql.Time) srcObj).getTime());
				} else {
					return new Timestamp(((java.sql.Date) srcObj).getTime());
				}
			case java.sql.Types.TIME:
				if (srcType == java.sql.Types.TIMESTAMP) {
					return new Time(((java.sql.Timestamp) srcObj).getTime());
				} else {
					throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
				}
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
				return srcObj.toString();
			default:
				throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
			}
		} catch (NumberFormatException ex) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

	}

	/**
	 * Indicates whether the given SQL data type is a temporal type. This method is
	 * called internally by the conversion methods <code>convertNumeric</code> and
	 * <code>convertTemporal</code>.
	 *
	 * @param type one of the constants from <code>java.sql.Types</code>
	 * @return <code>true</code> if the given type is <code>DATE</code>,
	 *         <code>TIME</code>, or <code>TIMESTAMP</code>; <code>false</code>
	 *         otherwise
	 */
	private boolean isTemporal(int type) {
		switch (type) {
		case java.sql.Types.DATE:
		case java.sql.Types.TIME:
		case java.sql.Types.TIMESTAMP:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Retrieves the type map associated with the <code>Connection</code> object for
	 * this <code>RowSet</code> object.
	 * <P>
	 * Drivers that support the JDBC 3.0 API will create <code>Connection</code>
	 * objects with an associated type map. This type map, which is initially empty,
	 * can contain one or more fully-qualified SQL names and <code>Class</code>
	 * objects indicating the class to which the named SQL value will be mapped. The
	 * type mapping specified in the connection's type map is used for custom type
	 * mapping when no other type map supersedes it.
	 * <p>
	 * If a type map is explicitly supplied to a method that can perform custom
	 * mapping, that type map supersedes the connection's type map.
	 *
	 * @return the <code>java.util.Map</code> object that is the type map for this
	 *         <code>RowSet</code> object's connection
	 */
	public Map<String, Class<?>> getTypeMap() {
		return typeMap;
	}

	/**
	 * Installs the given <code>java.util.Map</code> object as the type map
	 * associated with the <code>Connection</code> object for this
	 * <code>RowSet</code> object. The custom mapping indicated in this type map
	 * will be used unless a different type map is explicitly supplied to a method,
	 * in which case the type map supplied will be used.
	 *
	 * @param map a <code>java.util.Map</code> object that contains the mapping from
	 *            SQL type names for user defined types (UDT) to classes in the Java
	 *            programming language. Each entry in the <code>Map</code> object
	 *            consists of the fully qualified SQL name of a UDT and the
	 *            <code>Class</code> object for the <code>SQLData</code>
	 *            implementation of that UDT. May be <code>null</code>.
	 */
	public void setTypeMap(Map<String, Class<?>> map) {
		this.typeMap = map;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		return value.toString();
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return false;
		}
		setLastValueNull(false);

		if (value instanceof Boolean) {
			return ((Boolean) value).booleanValue();
		}

		try {
			return Double.compare(Double.parseDouble(value.toString()), 0) != 0;
		} catch (NumberFormatException ex) {
			throw new SQLException(String.format(MESSAGE_FAILED_GET, "Double", value, columnIndex));
		}
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return 0;
		}
		setLastValueNull(false);

		try {
			return Byte.parseByte(value.toString());
		} catch (NumberFormatException ex) {
			throw new SQLException(String.format(MESSAGE_FAILED_GET, "Byte", value, columnIndex));
		}
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return 0;
		}
		setLastValueNull(false);

		try {
			return Short.parseShort(value.toString().trim());
		} catch (NumberFormatException ex) {
			throw new SQLException(String.format(MESSAGE_FAILED_GET, "Short", value, columnIndex));
		}

	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return 0;
		}
		setLastValueNull(false);

		try {
			return Integer.parseInt(value.toString().trim());
		} catch (NumberFormatException ex) {
			throw new SQLException(String.format(MESSAGE_FAILED_GET, "Int", value, columnIndex));
		}

	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return 0;
		}
		setLastValueNull(false);

		try {
			return Long.parseLong(value.toString().trim());
		} catch (NumberFormatException ex) {
			throw new SQLException(String.format(MESSAGE_FAILED_GET, "Long", value, columnIndex));
		}
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return 0;
		}
		setLastValueNull(false);

		try {
			return Float.parseFloat(value.toString().trim());
		} catch (NumberFormatException ex) {
			throw new SQLException(String.format(MESSAGE_FAILED_GET, "Float", value, columnIndex));
		}
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return 0;
		}
		setLastValueNull(false);

		try {
			return Double.parseDouble(value.toString().trim());
		} catch (NumberFormatException ex) {
			throw new SQLException(String.format(MESSAGE_FAILED_GET, "Double", value, columnIndex));
		}
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		if (getColumnObject(columnIndex) == null) {
			setLastValueNull(true);
			return new BigDecimal(0);
		}
		setLastValueNull(false);

		return getBigDecimal(columnIndex).setScale(scale);
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		if (!isBinary(metaData.getColumnType(columnIndex))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		return (byte[]) getColumnObject(columnIndex);
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		/*
		 * The object coming back from the db could be a date, a timestamp, or a char
		 * field variety. If it's a date type return it, a timestamp we turn into a long
		 * and then into a date, char strings we try to parse. Yuck.
		 */
		switch (metaData.getColumnType(columnIndex)) {
		case Types.DATE:
			return new Date(((Date) value).getTime());
		case Types.TIMESTAMP:
			return new Date(((Timestamp) value).getTime());
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			try {
				DateFormat df = DateFormat.getDateInstance();
				return (Date) df.parse(value.toString());
			} catch (ParseException ex) {
				throw new SQLException(String.format(MESSAGE_FAILED_GET_CONVERSION, "Date", value, columnIndex));
			}
		default:
			throw new SQLException(String.format(MESSAGE_FAILED_GET_CONVERSION, "Date", value, columnIndex));
		}
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		/*
		 * The object coming back from the db could be a date, a timestamp, or a char
		 * field variety. If it's a date type return it, a timestamp we turn into a long
		 * and then into a date, char strings we try to parse. Yuck.
		 */
		switch (metaData.getColumnType(columnIndex)) {
		case Types.TIME:
			return (Time) value;
		case Types.TIMESTAMP:
			return new Time(((Timestamp) value).getTime());
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			try {
				DateFormat tf = DateFormat.getTimeInstance();
				return (Time) tf.parse(value.toString());
			} catch (ParseException ex) {
				throw new SQLException(String.format(MESSAGE_FAILED_GET_CONVERSION, "Time", value, columnIndex));
			}
		default:
			throw new SQLException(String.format(MESSAGE_FAILED_GET_CONVERSION, "Time", value, columnIndex));
		}
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		/*
		 * The object coming back from the db could be a date, a timestamp, or a char
		 * field variety. If it's a date type return it; a timestamp we turn into a long
		 * and then into a date; char strings we try to parse. Yuck.
		 */
		switch (metaData.getColumnType(columnIndex)) {
		case Types.TIMESTAMP:
			return (Timestamp) value;
		case Types.TIME:
			return new Timestamp(((Time) value).getTime());
		case Types.DATE:
			return new Timestamp(((Date) value).getTime());
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			try {
				DateFormat tf = DateFormat.getTimeInstance();
				return (Timestamp) tf.parse(value.toString());
			} catch (ParseException ex) {
				throw new SQLException(String.format(MESSAGE_FAILED_GET_CONVERSION, "Timestamp", value, columnIndex));
			}
		default:
			throw new SQLException(String.format(MESSAGE_FAILED_GET_CONVERSION, "Timestamp", value, columnIndex));
		}
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		if (isString(metaData.getColumnType(columnIndex))) {
			return new ByteArrayInputStream(((String) value).getBytes(US_ASCII));
		}
		throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		if (!isBinary(metaData.getColumnType(columnIndex)) && !isString(metaData.getColumnType(columnIndex))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		return new StringBufferInputStream(value.toString());
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		if (!isBinary(metaData.getColumnType(columnIndex))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		return new ByteArrayInputStream((byte[]) value);
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return getString(getColumnIndex(columnLabel));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(getColumnIndex(columnLabel));
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return getByte(getColumnIndex(columnLabel));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return getShort(getColumnIndex(columnLabel));
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return getInt(getColumnIndex(columnLabel));
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return getLong(getColumnIndex(columnLabel));
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return getFloat(getColumnIndex(columnLabel));
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return getDouble(getColumnIndex(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return getBigDecimal(getColumnIndex(columnLabel), scale);
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		return getBytes(getColumnIndex(columnLabel));
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return getDate(getColumnIndex(columnLabel));
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return getTime(getColumnIndex(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(getColumnIndex(columnLabel));
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return getAsciiStream(getColumnIndex(columnLabel));
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return getUnicodeStream(getColumnIndex(columnLabel));
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return getBinaryStream(getColumnIndex(columnLabel));
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return sqlwarn;
	}

	@Override
	public void clearWarnings() throws SQLException {
		sqlwarn = null;
	}

	@Override
	public String getCursorName() throws SQLException {
		throw new SQLException("Positioned updates not supported");
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metaData;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {

		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		if (value instanceof Struct) {
			Struct struct = (Struct) value;
			Map<String, Class<?>> map = getTypeMap();
			// look up the class in the map
			Class<?> c = map.get(struct.getSQLTypeName());
			if (c != null) {
				// create new instance of the class
				SQLData obj;
				try {
					Object tmp = c.getConstructor().newInstance();
					obj = (SQLData) tmp;
				} catch (Exception ex) {
					throw new SQLException("Unable to Instantiate: ", ex);
				}
				// get the attributes from the struct
				Object[] attribs = struct.getAttributes(map);
				// create the SQLInput "stream"
				SQLInputImpl sqlInput = new SQLInputImpl(attribs, map);
				// read the values...
				obj.readSQL(sqlInput, struct.getSQLTypeName());
				return obj;
			}
		}
		return value;
	}

	@Override
	public boolean isClosed() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return getObject(getColumnIndex(columnLabel));
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		return getColumnIndex(columnLabel);
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		if (isBinary(metaData.getColumnType(columnIndex))) {
			Object value = getColumnObject(columnIndex);
			if (value == null) {
				setLastValueNull(true);
				return null;
			}
			setLastValueNull(false);
			return new InputStreamReader(new ByteArrayInputStream((byte[]) value));
		}
		if (isString(metaData.getColumnType(columnIndex))) {
			Object value = getColumnObject(columnIndex);
			if (value == null) {
				setLastValueNull(true);
				return null;
			}
			setLastValueNull(false);
			return new StringReader(value.toString());
		}
		throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		return getCharacterStream(getColumnIndex(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		try {
			return new BigDecimal(value.toString().trim());
		} catch (NumberFormatException ex) {
			throw new SQLException(String.format(MESSAGE_FAILED_GET, "BigDecimal", value, columnIndex));
		}
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(getColumnIndex(columnLabel));
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
	}

	@Override
	public int getFetchSize() throws SQLException {
		return 0;
	}

	@Override
	public int getType() throws SQLException {
		return ResultSet.TYPE_SCROLL_INSENSITIVE;
	}

	@Override
	public int getConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
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
		return null;
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		return null;
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		if (metaData.getColumnType(columnIndex) != Types.BLOB) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		Blob value = (Blob) getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		return value;
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		if (metaData.getColumnType(columnIndex) != Types.BLOB) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		Clob value = (Clob) getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		return value;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		if (metaData.getColumnType(columnIndex) != Types.ARRAY) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		Array value = (Array) getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		return value;
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		return getObject(getColumnIndex(columnLabel), map);
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		return getRef(getColumnIndex(columnLabel));
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		return getBlob(getColumnIndex(columnLabel));
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		return getClob(getColumnIndex(columnLabel));
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		return getArray(getColumnIndex(columnLabel));
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		value = convertTemporal(value, metaData.getColumnType(columnIndex), Types.DATE);

		// create a default calendar
		Calendar defaultCal = Calendar.getInstance();
		// set this Calendar to the time we have
		defaultCal.setTime((java.util.Date) value);

		/*
		 * Now we can pull the pieces of the date out of the default calendar and put
		 * them into the user provided calendar
		 */
		cal.set(Calendar.YEAR, defaultCal.get(Calendar.YEAR));
		cal.set(Calendar.MONTH, defaultCal.get(Calendar.MONTH));
		cal.set(Calendar.DAY_OF_MONTH, defaultCal.get(Calendar.DAY_OF_MONTH));

		/*
		 * This looks a little odd but it is correct - Calendar.getTime() returns a
		 * Date...
		 */
		return new Date(cal.getTime().getTime());

	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		return getDate(getColumnIndex(columnLabel), cal);
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		value = convertTemporal(value, metaData.getColumnType(columnIndex), Types.DATE);

		// create a default calendar
		Calendar defaultCal = Calendar.getInstance();
		// set the time in the default Calendar
		defaultCal.setTime((java.util.Date) value);

		/*
		 * Now we can pull the pieces of the date out of the default calendar and put
		 * them into the user provided calendar
		 */
		cal.set(Calendar.HOUR_OF_DAY, defaultCal.get(Calendar.HOUR_OF_DAY));
		cal.set(Calendar.MINUTE, defaultCal.get(Calendar.MINUTE));
		cal.set(Calendar.SECOND, defaultCal.get(Calendar.SECOND));

		/*
		 * This looks a little odd but it is correct - Calendar.getTime() returns a
		 * Date...
		 */
		return new Time(cal.getTime().getTime());
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		return getTime(getColumnIndex(columnLabel), cal);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		value = convertTemporal(value, metaData.getColumnType(columnIndex), Types.DATE);

		// create a default calendar
		Calendar defaultCal = Calendar.getInstance();
		// set the time in the default Calendar
		defaultCal.setTime((java.util.Date) value);

		/*
		 * Now we can pull the pieces of the date out of the default calendar and put
		 * them into the user provided calendar
		 */
		cal.set(Calendar.YEAR, defaultCal.get(Calendar.YEAR));
		cal.set(Calendar.MONTH, defaultCal.get(Calendar.MONTH));
		cal.set(Calendar.DAY_OF_MONTH, defaultCal.get(Calendar.DAY_OF_MONTH));
		cal.set(Calendar.HOUR_OF_DAY, defaultCal.get(Calendar.HOUR_OF_DAY));
		cal.set(Calendar.MINUTE, defaultCal.get(Calendar.MINUTE));
		cal.set(Calendar.SECOND, defaultCal.get(Calendar.SECOND));

		return new Timestamp(cal.getTime().getTime());
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		return getTimestamp(getColumnIndex(columnLabel), cal);
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		checkCursor();

		if (metaData.getColumnType(columnIndex) != java.sql.Types.DATALINK) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		Object value = getColumnObject(columnIndex);
		if (value == null) {
			setLastValueNull(true);
			return null;
		}
		setLastValueNull(false);

		return (URL) value;
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		return getURL(getColumnIndex(columnLabel));
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
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
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
		throw new SQLFeatureNotSupportedException();
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
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
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
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
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
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int hashCode() {
		return Objects.hash(metaData);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AbstractResultSet other = (AbstractResultSet) obj;
		return Objects.equals(metaData, other.metaData);
	}

}
