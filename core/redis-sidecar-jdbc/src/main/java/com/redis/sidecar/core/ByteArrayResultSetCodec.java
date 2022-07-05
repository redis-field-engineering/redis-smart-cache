package com.redis.sidecar.core;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

public class ByteArrayResultSetCodec implements RedisCodec<String, ResultSet> {

	private static final byte[] EMPTY = new byte[0];
	private static final StringCodec STRING_CODEC = StringCodec.UTF8;
	private static final Charset CHARSET = StandardCharsets.UTF_8;

	private final Timer encodeTimer;
	private final Timer decodeTimer;
	private final RowSetFactory rowSetFactory;
	private final int maxByteBufferCapacity;

	public ByteArrayResultSetCodec(RowSetFactory rowSetFactory, int maxByteBufferCapacity,
			MeterRegistry meterRegistry) {
		this.rowSetFactory = rowSetFactory;
		this.maxByteBufferCapacity = maxByteBufferCapacity;
		this.encodeTimer = meterRegistry.timer("encoding", "codec", "ByteArrayResultSetCodec");
		this.decodeTimer = meterRegistry.timer("decoding", "codec", "ByteArrayResultSetCodec");
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
			return decodeTimer.recordCallable(() -> {
				return decode(Unpooled.wrappedBuffer(bytes));
			});
		} catch (Exception e) {
			throw new IllegalStateException("Could not decode RowSet", e);
		}
	}

	private RowSet decode(ByteBuf byteBuf) throws SQLException {
		CachedRowSet rowSet = rowSetFactory.createCachedRowSet();
		rowSet.setMetaData(readMetaData(byteBuf));
		while (byteBuf.isReadable()) {
			rowSet.moveToInsertRow();
			decodeRow(byteBuf, rowSet);
			rowSet.insertRow();
		}
		rowSet.moveToCurrentRow();
		rowSet.beforeFirst();
		return rowSet;
	}

	public void decodeRow(ByteBuf byteBuf, CachedRowSet rowSet) throws SQLException {
		for (int column = 1; column <= rowSet.getMetaData().getColumnCount(); column++) {
			if (byteBuf.readBoolean()) {
				rowSet.updateNull(column);
				continue;
			}
			switch (rowSet.getMetaData().getColumnType(column)) {
			case Types.BIT:
			case Types.BOOLEAN:
				rowSet.updateBoolean(column, byteBuf.readBoolean());
				break;
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
				rowSet.updateInt(column, byteBuf.readInt());
				break;
			case Types.BIGINT:
				rowSet.updateLong(column, byteBuf.readLong());
				break;
			case Types.FLOAT:
			case Types.REAL:
				rowSet.updateFloat(column, byteBuf.readFloat());
				break;
			case Types.DOUBLE:
			case Types.NUMERIC:
			case Types.DECIMAL:
				rowSet.updateDouble(column, byteBuf.readDouble());
				break;
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.LONGNVARCHAR:
				rowSet.updateString(column, readString(byteBuf));
				break;
			case Types.DATE:
				rowSet.updateDate(column, new Date(byteBuf.readLong()));
				break;
			case Types.TIME:
			case Types.TIME_WITH_TIMEZONE:
				rowSet.updateTime(column, new Time(byteBuf.readLong()));
				break;
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:
				rowSet.updateTimestamp(column, new Timestamp(byteBuf.readLong()));
				break;
			case Types.ROWID:
				rowSet.updateRowId(column, new com.redis.sidecar.jdbc.RowId(readString(byteBuf)));
				break;
			case Types.CLOB:
				rowSet.updateClob(column, new com.redis.sidecar.jdbc.Clob(readString(byteBuf)));
				break;
			case Types.BINARY:
			case Types.BLOB:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				byte[] bytes = new byte[byteBuf.readInt()];
				byteBuf.readBytes(bytes);
				rowSet.updateBytes(column, bytes);
				break;
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
				throw new SQLException("Column type no supported: " + rowSet.getMetaData().getColumnType(column));
			}
		}
	}

	private RowSetMetaData readMetaData(ByteBuf bytes) throws SQLException {
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

	private String readString(ByteBuf buffer) {
		int length = buffer.readShort();
		return buffer.readCharSequence(length, CHARSET).toString();
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

	@Override
	public ByteBuffer encodeValue(ResultSet resultSet) {
		try {
			return encodeTimer.recordCallable(() -> {
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
			});
		} catch (Exception e) {
			throw new IllegalStateException("Could not encode ResultSet", e);
		}
	}

	public void encode(ResultSet resultSet, ByteBuf byteBuf) {
		try {
			writeMetaData(resultSet.getMetaData(), byteBuf);
			while (resultSet.next()) {
				encodeRow(resultSet, byteBuf);
			}
			resultSet.beforeFirst();
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	public void encodeRow(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
		for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
			int sqlType = resultSet.getMetaData().getColumnType(columnIndex);
			switch (sqlType) {
			case Types.BIT:
			case Types.BOOLEAN:
				boolean booleanValue = resultSet.getBoolean(columnIndex);
				if (resultSet.wasNull()) {
					byteBuf.writeBoolean(true);
				} else {
					byteBuf.writeBoolean(false);
					byteBuf.writeBoolean(booleanValue);
				}
				break;
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
				int intValue = resultSet.getInt(columnIndex);
				if (resultSet.wasNull()) {
					byteBuf.writeBoolean(true);
				} else {
					byteBuf.writeBoolean(false);
					byteBuf.writeInt(intValue);
				}
				break;
			case Types.BIGINT:
				long longValue = resultSet.getLong(columnIndex);
				if (resultSet.wasNull()) {
					byteBuf.writeBoolean(true);
				} else {
					byteBuf.writeBoolean(false);
					byteBuf.writeLong(longValue);
				}
				break;
			case Types.FLOAT:
			case Types.REAL:
				float floatValue = resultSet.getFloat(columnIndex);
				if (resultSet.wasNull()) {
					byteBuf.writeBoolean(true);
				} else {
					byteBuf.writeBoolean(false);
					byteBuf.writeFloat(floatValue);
				}
				break;
			case Types.DOUBLE:
				double doubleValue = resultSet.getDouble(columnIndex);
				if (resultSet.wasNull()) {
					byteBuf.writeBoolean(true);
				} else {
					byteBuf.writeBoolean(false);
					byteBuf.writeDouble(doubleValue);
				}
				break;
			case Types.NUMERIC:
			case Types.DECIMAL:
				Double bigDecimalDoubleValue = getBigDecimalDouble(resultSet, columnIndex);
				if (resultSet.wasNull() || bigDecimalDoubleValue == null) {
					byteBuf.writeBoolean(true);
				} else {
					byteBuf.writeBoolean(false);
					byteBuf.writeDouble(bigDecimalDoubleValue);
				}
				break;
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.LONGNVARCHAR:
				String string = resultSet.getString(columnIndex);
				if (resultSet.wasNull() || string == null) {
					byteBuf.writeBoolean(true);
				} else {
					byteBuf.writeBoolean(false);
					writeString(byteBuf, string);
				}
				break;
			case Types.DATE:
			case Types.TIME:
			case Types.TIME_WITH_TIMEZONE:
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:
				Long timestamp = getLong(resultSet, columnIndex);
				if (resultSet.wasNull() || timestamp == null) {
					byteBuf.writeBoolean(true);
				} else {
					byteBuf.writeBoolean(false);
					byteBuf.writeLong(timestamp);
				}
				break;
			case Types.ROWID:
				RowId rowId = resultSet.getRowId(columnIndex);
				if (resultSet.wasNull() || rowId == null) {
					byteBuf.writeBoolean(true);
				} else {
					byteBuf.writeBoolean(false);
					writeString(byteBuf, rowId.toString());
				}
				break;
			case Types.CLOB:
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
					try {
						clob.free();
					} catch (AbstractMethodError e) {
						// May occur with old JDBC drivers
					}
				}
				break;
			case Types.BINARY:
			case Types.BLOB:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				byte[] bytes = resultSet.getBytes(columnIndex);
				if (resultSet.wasNull() || bytes == null) {
					byteBuf.writeBoolean(true);
				} else {
					byteBuf.writeBoolean(false);
					byteBuf.writeInt(bytes.length);
					byteBuf.writeBytes(bytes);
				}
				break;
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
	}

	private Double getBigDecimalDouble(ResultSet resultSet, int columnIndex) throws SQLException {
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

}
