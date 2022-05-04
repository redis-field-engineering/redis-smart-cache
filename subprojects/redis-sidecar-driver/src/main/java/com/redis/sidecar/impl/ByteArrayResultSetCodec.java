package com.redis.sidecar.impl;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

public class ByteArrayResultSetCodec implements RedisCodec<String, ResultSet> {

	private static final byte[] EMPTY = new byte[0];
	private static final StringCodec STRING_CODEC = StringCodec.UTF8;
	private static final Charset CHARSET = StandardCharsets.UTF_8;

	private final int maxByteBufferCapacity;

	public ByteArrayResultSetCodec(int maxByteBufferCapacity) {
		this.maxByteBufferCapacity = maxByteBufferCapacity;
	}

	@Override
	public String decodeKey(ByteBuffer bytes) {
		return STRING_CODEC.decodeKey(bytes);
	}

	@Override
	public ByteBuffer encodeKey(String key) {
		return STRING_CODEC.encodeKey(key);
	}

	@Override
	public ResultSet decodeValue(ByteBuffer bytes) {
		ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
		CachedResultSetMetaData metaData = readMetaData(byteBuf);
		List<List<Object>> rows = new ArrayList<>();
		int rowIndex = 1;
		while (byteBuf.isReadable()) {
			if (byteBuf.readInt() != rowIndex) {
				throw new IllegalStateException("Missing ResultSet row #" + rowIndex);
			}
			try {
				List<Object> row = new ArrayList<>(metaData.getColumnCount());
				for (ColumnMetaData column : metaData.getColumns()) {
					row.add(readValue(byteBuf, column.getColumnType()));
				}
				rows.add(row);
			} catch (SQLException e) {
				throw new IllegalStateException("Could not decode row", e);
			}
			rowIndex++;
		}
		return new ListResultSet(metaData, rows);
	}

	private CachedResultSetMetaData readMetaData(ByteBuf bytes) {
		int columnCount = bytes.readInt();
		List<ColumnMetaData> columns = new ArrayList<>(columnCount);
		for (int index = 0; index < columnCount; index++) {
			ColumnMetaData column = new ColumnMetaData();
			column.setCatalogName(readString(bytes));
			column.setColumnClassName(readString(bytes));
			column.setColumnLabel(readString(bytes));
			column.setColumnName(readString(bytes));
			column.setColumnTypeName(readString(bytes));
			column.setColumnType(bytes.readInt());
			column.setColumnDisplaySize(bytes.readInt());
			column.setPrecision(bytes.readInt());
			column.setTableName(readString(bytes));
			column.setScale(bytes.readInt());
			column.setSchemaName(readString(bytes));
			column.setAutoIncrement(bytes.readBoolean());
			column.setCaseSensitive(bytes.readBoolean());
			column.setCurrency(bytes.readBoolean());
			column.setDefinitelyWritable(bytes.readBoolean());
			column.setIsNullable(bytes.readInt());
			column.setReadOnly(bytes.readBoolean());
			column.setSearchable(bytes.readBoolean());
			column.setSigned(bytes.readBoolean());
			column.setWritable(bytes.readBoolean());
			columns.add(column);
		}
		return new CachedResultSetMetaData(columns);
	}

	private String readString(ByteBuf buffer) {
		int length = buffer.readShort();
		return buffer.readCharSequence(length, CHARSET).toString();
	}

	private byte[] readBytes(ByteBuf buffer) {
		byte[] bytes = new byte[buffer.readInt()];
		buffer.readBytes(bytes);
		return bytes;
	}

	private void writeString(ByteBuf buffer, String value) {
		ByteBuf buf = buffer.alloc().buffer(ByteBufUtil.utf8MaxBytes(value));
		try {
			int length = ByteBufUtil.writeUtf8(buf, value);
			buffer.writeShort(length);
			buffer.writeBytes(buf);
		} finally {
			buf.release();
		}
	}

	private void writeBytes(ByteBuf buffer, byte[] value) {
		buffer.writeInt(value.length);
		buffer.writeBytes(value);
	}

	private Object readValue(ByteBuf bytes, int sqlType) throws SQLException {
		if (bytes.readBoolean()) {
			return null;
		}
		switch (sqlType) {
		case Types.BIT:
		case Types.BOOLEAN:
			return bytes.readBoolean();
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			return bytes.readInt();
		case Types.BIGINT:
			return bytes.readLong();
		case Types.FLOAT:
		case Types.REAL:
			return bytes.readFloat();
		case Types.DOUBLE:
		case Types.NUMERIC:
		case Types.DECIMAL:
			return bytes.readDouble();
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGNVARCHAR:
			return readString(bytes);
		case Types.DATE:
			return new Date(bytes.readLong());
		case Types.TIME:
		case Types.TIME_WITH_TIMEZONE:
			return new Time(bytes.readLong());
		case Types.TIMESTAMP:
		case Types.TIMESTAMP_WITH_TIMEZONE:
			return new Timestamp(bytes.readLong());
		case Types.ROWID:
			return readString(bytes);
		case Types.CLOB:
			return readString(bytes);
		case Types.BINARY:
		case Types.BLOB:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return readBytes(bytes);
		case Types.NULL:
		case Types.OTHER:
		case Types.JAVA_OBJECT:
		case Types.DISTINCT:
		case Types.STRUCT:
		case Types.ARRAY:
		case Types.REF:
		case Types.DATALINK:
		case Types.NCLOB:
		case Types.SQLXML:
		case Types.REF_CURSOR:
		default:
			throw new SQLException("Column type no supported: " + sqlType);
		}
	}

	@Override
	public ByteBuffer encodeValue(ResultSet resultSet) {
		if (resultSet == null) {
			return ByteBuffer.wrap(EMPTY);
		}
		ByteBuffer buffer = ByteBuffer.allocate(maxByteBufferCapacity);
		ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer);
		byteBuf.clear();
		encode(resultSet, byteBuf);
		int writerIndex = byteBuf.writerIndex();
		buffer.limit(writerIndex);
		return buffer;
	}

	public void encode(ResultSet resultSet, ByteBuf byteBuf) {
		try {
			ResultSetMetaData metaData = resultSet.getMetaData();
			writeMetaData(metaData, byteBuf);
			int columnCount = metaData.getColumnCount();
			int rowIndex = 1;
			while (resultSet.next()) {
				byteBuf.writeInt(rowIndex);
				for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
					writeValue(resultSet, columnIndex, byteBuf);
				}
				rowIndex++;
			}
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	private void writeValue(ResultSet resultSet, int columnIndex, ByteBuf out) throws SQLException {
		int sqlType = resultSet.getMetaData().getColumnType(columnIndex);
		switch (sqlType) {
		case Types.BIT:
		case Types.BOOLEAN:
			write(resultSet, resultSet.getBoolean(columnIndex), out, out::writeBoolean);
			return;
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			write(resultSet, resultSet.getInt(columnIndex), out, out::writeInt);
			return;
		case Types.BIGINT:
			write(resultSet, resultSet.getLong(columnIndex), out, out::writeLong);
			return;
		case Types.FLOAT:
		case Types.REAL:
			write(resultSet, resultSet.getFloat(columnIndex), out, out::writeFloat);
			return;
		case Types.DOUBLE:
			write(resultSet, resultSet.getDouble(columnIndex), out, out::writeDouble);
			return;
		case Types.NUMERIC:
		case Types.DECIMAL:
			write(resultSet, getDouble(resultSet, columnIndex), out, out::writeDouble);
			return;
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGNVARCHAR:
			write(resultSet, resultSet.getString(columnIndex), out, v -> writeString(out, v));
			return;
		case Types.DATE:
		case Types.TIME:
		case Types.TIME_WITH_TIMEZONE:
		case Types.TIMESTAMP:
		case Types.TIMESTAMP_WITH_TIMEZONE:
			write(resultSet, getLong(resultSet, columnIndex), out, out::writeLong);
			return;
		case Types.ROWID:
			write(resultSet, resultSet.getRowId(columnIndex), out, v -> writeString(out, v.toString()));
			return;
		case Types.CLOB:
			write(resultSet, getString(resultSet, columnIndex), out, v -> writeString(out, v));
			return;
		case Types.BINARY:
		case Types.BLOB:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			write(resultSet, resultSet.getBytes(columnIndex), out, v -> writeBytes(out, v));
			return;
		case Types.NULL:
		case Types.OTHER:
		case Types.JAVA_OBJECT:
		case Types.DISTINCT:
		case Types.STRUCT:
		case Types.ARRAY:
		case Types.REF:
		case Types.DATALINK:
		case Types.NCLOB:
		case Types.SQLXML:
		case Types.REF_CURSOR:
		default:
			throw new SQLException("Column type no supported: " + sqlType);
		}
	}

	private String getString(ResultSet resultSet, int columnIndex) throws SQLException {
		Clob clob = resultSet.getClob(columnIndex);
		if (clob == null) {
			return null;
		}
		try {
			int size;
			try {
				size = Math.toIntExact(clob.length());
			} catch (ArithmeticException e) {
				throw new SQLException("CLOB is too large", e);
			}
			if (size == 0) {
				return "";
			}
			return clob.getSubString(1, size);
		} finally {
			try {
				clob.free();
			} catch (AbstractMethodError e) {
				// May occur with old JDBC drivers
			}
		}
	}

	private Double getDouble(ResultSet resultSet, int columnIndex) throws SQLException {
		BigDecimal value = resultSet.getBigDecimal(columnIndex);
		if (value == null) {
			return null;
		}
		return value.doubleValue();
	}

	private Long getLong(ResultSet resultSet, int columnIndex) throws SQLException {
		Object value = resultSet.getObject(columnIndex);
		if (value == null) {
			return null;
		}
		if (value instanceof java.util.Date) {
			return ((java.util.Date) value).getTime();
		}
		if (value instanceof LocalDateTime) {
			return ((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
		}
		if (value instanceof Number) {
			return ((Number) value).longValue();
		}
		try {
			return DateFormat.getDateInstance().parse(value.toString()).getTime();
		} catch (ParseException e) {
			throw new SQLException(String.format("Unexpected value '%s' on column %s", value, columnIndex), e);
		}
	}

	private void writeMetaData(ResultSetMetaData metaData, ByteBuf bytes) throws SQLException {
		bytes.writeInt(metaData.getColumnCount());
		for (int index = 1; index <= metaData.getColumnCount(); index++) {
			writeString(bytes, metaData.getCatalogName(index));
			writeString(bytes, metaData.getColumnClassName(index));
			writeString(bytes, metaData.getColumnLabel(index));
			writeString(bytes, metaData.getColumnName(index));
			writeString(bytes, metaData.getColumnTypeName(index));
			bytes.writeInt(metaData.getColumnType(index));
			bytes.writeInt(metaData.getColumnDisplaySize(index));
			bytes.writeInt(metaData.getPrecision(index));
			writeString(bytes, metaData.getTableName(index));
			bytes.writeInt(metaData.getScale(index));
			writeString(bytes, metaData.getSchemaName(index));
			bytes.writeBoolean(metaData.isAutoIncrement(index));
			bytes.writeBoolean(metaData.isCaseSensitive(index));
			bytes.writeBoolean(metaData.isCurrency(index));
			bytes.writeBoolean(metaData.isDefinitelyWritable(index));
			bytes.writeInt(metaData.isNullable(index));
			bytes.writeBoolean(metaData.isReadOnly(index));
			bytes.writeBoolean(metaData.isSearchable(index));
			bytes.writeBoolean(metaData.isSigned(index));
			bytes.writeBoolean(metaData.isWritable(index));
		}
	}

	private interface ByteBufWriter<T> {

		void write(T value);

	}

	private <T> void write(ResultSet resultSet, T value, ByteBuf output, ByteBufWriter<T> writer) throws SQLException {
		boolean wasNull = resultSet.wasNull() || value == null;
		output.writeBoolean(wasNull);
		if (!wasNull) {
			writer.write(value);
		}
	}

}
