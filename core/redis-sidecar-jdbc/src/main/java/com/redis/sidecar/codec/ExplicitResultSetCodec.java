package com.redis.sidecar.codec;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;

import javax.sql.RowSet;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetMetaDataImpl;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ExplicitResultSetCodec implements RedisCodec<String, ResultSet> {

	private static final byte[] EMPTY = new byte[0];
	private static final StringCodec STRING_CODEC = StringCodec.UTF8;

	private final RowSetFactory rowSetFactory;
	private final int maxByteBufferCapacity;

	public ExplicitResultSetCodec(RowSetFactory rowSetFactory, int maxByteBufferCapacity) {
		this.rowSetFactory = rowSetFactory;
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
	public RowSet decodeValue(ByteBuffer bytes) {
		try {
			return decodeRowSet(Unpooled.wrappedBuffer(bytes));
		} catch (SQLException e) {
			throw new IllegalStateException("Could not decode RowSet", e);
		}
	}

	public RowSet decodeRowSet(ByteBuf byteBuf) throws SQLException {
		CachedRowSet rowSet = rowSetFactory.createCachedRowSet();
		RowSetMetaData metaData = decodeMetaData(byteBuf);
		rowSet.setMetaData(metaData);
		int columnCount = metaData.getColumnCount();
		ColumnDecoder[] columnDecoders = new ColumnDecoder[columnCount];
		for (int index = 0; index < columnCount; index++) {
			int columnIndex = index + 1;
			int columnType = metaData.getColumnType(columnIndex);
			columnDecoders[index] = columnDecoder(columnIndex, columnType);
		}
		while (byteBuf.isReadable()) {
			rowSet.moveToInsertRow();
			for (int index = 0; index < columnDecoders.length; index++) {
				boolean nullValue = byteBuf.readBoolean();
				if (nullValue) {
					rowSet.updateNull(index + 1);
				} else {
					columnDecoders[index].decode(byteBuf, rowSet);
				}
			}
			rowSet.insertRow();
		}
		rowSet.moveToCurrentRow();
		rowSet.beforeFirst();
		return rowSet;
	}

	interface ColumnDecoder {

		void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException;

	}

	interface ColumnEncoder {

		void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException;

	}

	static class BooleanColumnDecoder implements ColumnDecoder {

		private final int columnIndex;

		public BooleanColumnDecoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateBoolean(columnIndex, byteBuf.readBoolean());
		}
	}

	static class IntegerColumnDecoder implements ColumnDecoder {

		private final int columnIndex;

		public IntegerColumnDecoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateInt(columnIndex, byteBuf.readInt());
		}
	}

	static class LongColumnDecoder implements ColumnDecoder {

		private final int columnIndex;

		public LongColumnDecoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateLong(columnIndex, byteBuf.readLong());
		}
	}

	static class FloatColumnDecoder implements ColumnDecoder {

		private final int columnIndex;

		public FloatColumnDecoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateFloat(columnIndex, byteBuf.readFloat());
		}
	}

	static class DoubleColumnDecoder implements ColumnDecoder {

		private final int columnIndex;

		public DoubleColumnDecoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateDouble(columnIndex, byteBuf.readDouble());
		}
	}

	static class StringColumnDecoder implements ColumnDecoder {

		private final int columnIndex;

		public StringColumnDecoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateString(columnIndex, readString(byteBuf));
		}
	}

	static class DateColumnDecoder implements ColumnDecoder {

		private final int columnIndex;

		public DateColumnDecoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateDate(columnIndex, new Date(byteBuf.readLong()));
		}
	}

	static class TimeColumnDecoder implements ColumnDecoder {

		private final int columnIndex;

		public TimeColumnDecoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateTime(columnIndex, new Time(byteBuf.readLong()));
		}
	}

	static class TimestampColumnDecoder implements ColumnDecoder {

		private final int columnIndex;

		public TimestampColumnDecoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateTimestamp(columnIndex, new Timestamp(byteBuf.readLong()));
		}
	}

	static class RowIdColumnDecoder implements ColumnDecoder {

		private final int columnIndex;

		public RowIdColumnDecoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateRowId(columnIndex, new com.redis.sidecar.jdbc.RowId(readString(byteBuf)));
		}
	}

	static class ClobColumnDecoder implements ColumnDecoder {

		private final int columnIndex;

		public ClobColumnDecoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateClob(columnIndex, new com.redis.sidecar.jdbc.Clob(readString(byteBuf)));
		}
	}

	static class BytesColumnDecoder implements ColumnDecoder {

		private final int columnIndex;

		public BytesColumnDecoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			byte[] bytes = new byte[byteBuf.readInt()];
			byteBuf.readBytes(bytes);
			resultSet.updateBytes(columnIndex, bytes);
		}
	}

	private ColumnDecoder columnDecoder(int columnIndex, int columnType) throws SQLException {
		switch (columnType) {
		case Types.BIT:
		case Types.BOOLEAN:
			return new BooleanColumnDecoder(columnIndex);
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			return new IntegerColumnDecoder(columnIndex);
		case Types.BIGINT:
			return new LongColumnDecoder(columnIndex);
		case Types.FLOAT:
		case Types.REAL:
			return new FloatColumnDecoder(columnIndex);
		case Types.DOUBLE:
		case Types.NUMERIC:
		case Types.DECIMAL:
			return new DoubleColumnDecoder(columnIndex);
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGNVARCHAR:
			return new StringColumnDecoder(columnIndex);
		case Types.DATE:
			return new DateColumnDecoder(columnIndex);
		case Types.TIME:
		case Types.TIME_WITH_TIMEZONE:
			return new TimeColumnDecoder(columnIndex);
		case Types.TIMESTAMP:
		case Types.TIMESTAMP_WITH_TIMEZONE:
			return new TimestampColumnDecoder(columnIndex);
		case Types.ROWID:
			return new RowIdColumnDecoder(columnIndex);
		case Types.CLOB:
			return new ClobColumnDecoder(columnIndex);
		case Types.BINARY:
		case Types.BLOB:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return new BytesColumnDecoder(columnIndex);
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
			throw new SQLException("Column type no supported: " + columnType);
		}
	}

	public RowSetMetaData decodeMetaData(ByteBuffer bytes) throws SQLException {
		return decodeMetaData(Unpooled.wrappedBuffer(bytes));
	}

	private RowSetMetaData decodeMetaData(ByteBuf bytes) throws SQLException {
		RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
		int columnCount = bytes.readInt();
		metaData.setColumnCount(columnCount);
		for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
			metaData.setCatalogName(columnIndex, readString(bytes));
			metaData.setColumnLabel(columnIndex, readString(bytes));
			metaData.setColumnName(columnIndex, readString(bytes));
			metaData.setColumnTypeName(columnIndex, readString(bytes));
			metaData.setColumnType(columnIndex, bytes.readInt());
			metaData.setColumnDisplaySize(columnIndex, bytes.readInt());
			metaData.setPrecision(columnIndex, bytes.readInt());
			metaData.setTableName(columnIndex, readString(bytes));
			metaData.setScale(columnIndex, bytes.readInt());
			metaData.setSchemaName(columnIndex, readString(bytes));
			metaData.setAutoIncrement(columnIndex, bytes.readBoolean());
			metaData.setCaseSensitive(columnIndex, bytes.readBoolean());
			metaData.setCurrency(columnIndex, bytes.readBoolean());
			metaData.setNullable(columnIndex, bytes.readInt());
			metaData.setSearchable(columnIndex, bytes.readBoolean());
			metaData.setSigned(columnIndex, bytes.readBoolean());
		}
		return metaData;
	}

	private static String readString(ByteBuf buffer) {
		int length = buffer.readInt();
		byte[] bytes = new byte[length];
		buffer.readBytes(bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}

	@Override
	public ByteBuffer encodeValue(ResultSet resultSet) {
		try {
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
		} catch (SQLException e) {
			throw new IllegalStateException("Could not encode ResultSet", e);
		}
	}

	public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
		ResultSetMetaData metaData = resultSet.getMetaData();
		encode(metaData, byteBuf);
		int columnCount = metaData.getColumnCount();
		ColumnEncoder[] encoders = new ColumnEncoder[columnCount];
		for (int index = 0; index < columnCount; index++) {
			int columnIndex = index + 1;
			int columnType = resultSet.getMetaData().getColumnType(columnIndex);
			encoders[index] = columnEncoder(columnIndex, columnType);
		}
		while (resultSet.next()) {
			for (int index = 0; index < encoders.length; index++) {
				encoders[index].encode(resultSet, byteBuf);
			}
		}
	}

	static class BooleanColumnEncoder implements ColumnEncoder {

		private final int columnIndex;

		public BooleanColumnEncoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
			boolean booleanValue = resultSet.getBoolean(columnIndex);
			if (resultSet.wasNull()) {
				byteBuf.writeBoolean(true);
			} else {
				byteBuf.writeBoolean(false);
				byteBuf.writeBoolean(booleanValue);
			}
		}

	}

	static class IntegerColumnEncoder implements ColumnEncoder {

		private final int columnIndex;

		public IntegerColumnEncoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
			int intValue = resultSet.getInt(columnIndex);
			if (resultSet.wasNull()) {
				byteBuf.writeBoolean(true);
			} else {
				byteBuf.writeBoolean(false);
				byteBuf.writeInt(intValue);
			}
		}

	}

	static class LongColumnEncoder implements ColumnEncoder {

		private final int columnIndex;

		public LongColumnEncoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
			long longValue = resultSet.getLong(columnIndex);
			if (resultSet.wasNull()) {
				byteBuf.writeBoolean(true);
			} else {
				byteBuf.writeBoolean(false);
				byteBuf.writeLong(longValue);
			}
		}

	}

	static class FloatColumnEncoder implements ColumnEncoder {

		private final int columnIndex;

		public FloatColumnEncoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
			float floatValue = resultSet.getFloat(columnIndex);
			if (resultSet.wasNull()) {
				byteBuf.writeBoolean(true);
			} else {
				byteBuf.writeBoolean(false);
				byteBuf.writeFloat(floatValue);
			}
		}

	}

	static class DoubleColumnEncoder implements ColumnEncoder {

		private final int columnIndex;

		public DoubleColumnEncoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
			double doubleValue = resultSet.getDouble(columnIndex);
			if (resultSet.wasNull()) {
				byteBuf.writeBoolean(true);
			} else {
				byteBuf.writeBoolean(false);
				byteBuf.writeDouble(doubleValue);
			}
		}

	}

	static class DecimalColumnEncoder implements ColumnEncoder {

		private final int columnIndex;

		public DecimalColumnEncoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
			BigDecimal value = resultSet.getBigDecimal(columnIndex);
			if (resultSet.wasNull() || value == null) {
				byteBuf.writeBoolean(true);
			} else {
				byteBuf.writeBoolean(false);
				byteBuf.writeDouble(value.doubleValue());
			}
		}

	}

	static class StringColumnEncoder implements ColumnEncoder {

		private final int columnIndex;

		public StringColumnEncoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
			String string = resultSet.getString(columnIndex);
			if (resultSet.wasNull() || string == null) {
				byteBuf.writeBoolean(true);
			} else {
				byteBuf.writeBoolean(false);
				writeString(byteBuf, string);
			}
		}

	}

	static class DateColumnEncoder implements ColumnEncoder {

		private final int columnIndex;

		public DateColumnEncoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
			Long timestamp = getLong(resultSet, columnIndex);
			if (resultSet.wasNull() || timestamp == null) {
				byteBuf.writeBoolean(true);
			} else {
				byteBuf.writeBoolean(false);
				byteBuf.writeLong(timestamp);
			}
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

	}

	static class RowIdColumnEncoder implements ColumnEncoder {

		private final int columnIndex;

		public RowIdColumnEncoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
			RowId rowId = resultSet.getRowId(columnIndex);
			if (resultSet.wasNull() || rowId == null) {
				byteBuf.writeBoolean(true);
			} else {
				byteBuf.writeBoolean(false);
				writeString(byteBuf, rowId.toString());
			}
		}

	}

	static class ClobColumnEncoder implements ColumnEncoder {

		private final int columnIndex;

		public ClobColumnEncoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
			Clob clob = resultSet.getClob(columnIndex);
			try {
				if (resultSet.wasNull() || clob == null) {
					byteBuf.writeBoolean(true);
				} else {
					int size;
					try {
						size = Math.toIntExact(clob.length());
					} catch (ArithmeticException e) {
						throw new SQLException("CLOB is too large", e);
					}
					String clobString = size == 0 ? "" : clob.getSubString(1, size);
					byteBuf.writeBoolean(false);
					writeString(byteBuf, clobString);
				}
			} finally {
				if (clob != null) {
					try {
						clob.free();
					} catch (AbstractMethodError e) {
						// May occur with old JDBC drivers
					}
				}
			}
		}
	}

	static class BlobColumnEncoder implements ColumnEncoder {

		private final int columnIndex;

		public BlobColumnEncoder(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
			byte[] bytes = resultSet.getBytes(columnIndex);
			if (resultSet.wasNull() || bytes == null) {
				byteBuf.writeBoolean(true);
			} else {
				byteBuf.writeBoolean(false);
				byteBuf.writeInt(bytes.length);
				byteBuf.writeBytes(bytes);
			}
		}
	}

	private ColumnEncoder columnEncoder(int columnIndex, int columnType) throws SQLException {
		switch (columnType) {
		case Types.BIT:
		case Types.BOOLEAN:
			return new BooleanColumnEncoder(columnIndex);
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			return new IntegerColumnEncoder(columnIndex);
		case Types.BIGINT:
			return new LongColumnEncoder(columnIndex);
		case Types.FLOAT:
		case Types.REAL:
			return new FloatColumnEncoder(columnIndex);
		case Types.DOUBLE:
			return new DoubleColumnEncoder(columnIndex);
		case Types.NUMERIC:
		case Types.DECIMAL:
			return new DecimalColumnEncoder(columnIndex);
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGNVARCHAR:
			return new StringColumnEncoder(columnIndex);
		case Types.DATE:
		case Types.TIME:
		case Types.TIME_WITH_TIMEZONE:
		case Types.TIMESTAMP:
		case Types.TIMESTAMP_WITH_TIMEZONE:
			return new DateColumnEncoder(columnIndex);
		case Types.ROWID:
			return new RowIdColumnEncoder(columnIndex);
		case Types.CLOB:
			return new ClobColumnEncoder(columnIndex);
		case Types.BINARY:
		case Types.BLOB:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return new BlobColumnEncoder(columnIndex);
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
			throw new SQLException("Column type no supported: " + columnType);
		}
	}

	public void encode(ResultSetMetaData metaData, ByteBuf bytes) throws SQLException {
		bytes.writeInt(metaData.getColumnCount());
		for (int index = 1; index <= metaData.getColumnCount(); index++) {
			writeString(bytes, metaData.getCatalogName(index));
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
			bytes.writeInt(metaData.isNullable(index));
			bytes.writeBoolean(metaData.isSearchable(index));
			bytes.writeBoolean(metaData.isSigned(index));
		}
	}

	private static void writeString(ByteBuf buf, String string) {
		byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
		buf.writeInt(bytes.length);
		buf.writeBytes(bytes);
	}

}
