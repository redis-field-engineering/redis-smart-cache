package com.redis.sidecar.codec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import javax.sql.RowSet;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetMetaDataImpl;

import com.redis.sidecar.codec.column.BigDecimalColumnCodec;
import com.redis.sidecar.codec.column.BinaryColumnCodec;
import com.redis.sidecar.codec.column.BlobColumnCodec;
import com.redis.sidecar.codec.column.BooleanColumnCodec;
import com.redis.sidecar.codec.column.DateColumnCodec;
import com.redis.sidecar.codec.column.DoubleColumnCodec;
import com.redis.sidecar.codec.column.FloatColumnCodec;
import com.redis.sidecar.codec.column.IntegerColumnCodec;
import com.redis.sidecar.codec.column.LongColumnCodec;
import com.redis.sidecar.codec.column.StringColumnCodec;
import com.redis.sidecar.codec.column.TimeColumnCodec;
import com.redis.sidecar.codec.column.TimestampColumnCodec;
import com.redis.sidecar.rowset.SidecarRowSetFactory;

import io.lettuce.core.codec.RedisCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

public class ResultSetCodec implements RedisCodec<String, ResultSet> {

	public static final int DEFAULT_BYTE_BUFFER_CAPACITY = 10000000; // 10 MB
	public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
	public static final String EMPTY_STRING = "";

	private final RowSetFactory rowSetFactory;
	private final int maxByteBufferCapacity;

	private ResultSetCodec(Builder builder) {
		this.rowSetFactory = builder.rowSetFactory;
		this.maxByteBufferCapacity = builder.maxByteBufferCapacity;
	}

	@Override
	public String decodeKey(ByteBuffer bytes) {
		return io.lettuce.core.codec.StringCodec.UTF8.decodeKey(bytes);
	}

	@Override
	public ByteBuffer encodeKey(String key) {
		return io.lettuce.core.codec.StringCodec.UTF8.encodeKey(key);
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
		ColumnCodec[] columnCodec = new ColumnCodec[columnCount];
		for (int index = 0; index < columnCount; index++) {
			int columnIndex = index + 1;
			int columnType = metaData.getColumnType(columnIndex);
			columnCodec[index] = columnCodec(columnIndex, columnType);
		}
		while (byteBuf.isReadable()) {
			rowSet.moveToInsertRow();
			for (int index = 0; index < columnCodec.length; index++) {
				columnCodec[index].decode(byteBuf, rowSet);
			}
			rowSet.insertRow();
		}
		rowSet.moveToCurrentRow();
		rowSet.beforeFirst();
		return rowSet;
	}

	private ColumnCodec columnCodec(int columnIndex, int columnType) throws SQLException {
		switch (columnType) {
		case Types.BIT:
		case Types.BOOLEAN:
			return new BooleanColumnCodec(columnIndex);
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			return new IntegerColumnCodec(columnIndex);
		case Types.BIGINT:
			return new LongColumnCodec(columnIndex);
		case Types.FLOAT:
		case Types.REAL:
			return new FloatColumnCodec(columnIndex);
		case Types.DOUBLE:
			return new DoubleColumnCodec(columnIndex);
		case Types.NUMERIC:
		case Types.DECIMAL:
			return new BigDecimalColumnCodec(columnIndex);
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGNVARCHAR:
			return new StringColumnCodec(columnIndex);
		case Types.DATE:
			return new DateColumnCodec(columnIndex);
		case Types.TIME:
			return new TimeColumnCodec(columnIndex);
		case Types.TIMESTAMP:
			return new TimestampColumnCodec(columnIndex);
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return new BinaryColumnCodec(columnIndex);
		case Types.BLOB:
			return new BlobColumnCodec(columnIndex);
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

	@Override
	public ByteBuffer encodeValue(ResultSet resultSet) {
		try {
			if (resultSet == null) {
				return ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
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
		ColumnCodec[] codecs = new ColumnCodec[columnCount];
		for (int index = 0; index < columnCount; index++) {
			int columnIndex = index + 1;
			int columnType = resultSet.getMetaData().getColumnType(columnIndex);
			codecs[index] = columnCodec(columnIndex, columnType);
		}
		while (resultSet.next()) {
			for (int index = 0; index < codecs.length; index++) {
				codecs[index].encode(resultSet, byteBuf);
			}
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

	public static String readString(ByteBuf byteBuf) {
		int length = byteBuf.readInt();
		if (length == 0) {
			return EMPTY_STRING;
		}
		return byteBuf.readCharSequence(length, StandardCharsets.UTF_8).toString();
	}

	public static void writeString(ByteBuf byteBuf, String string) {
		if (string.isEmpty()) {
			byteBuf.writeInt(0);
		} else {
			int length = ByteBufUtil.utf8Bytes(string);
			byteBuf.writeInt(length);
			ByteBufUtil.reserveAndWriteUtf8(byteBuf, string, length);
		}
	}

	public static Builder builder() throws SQLException {
		return new Builder();
	}

	public static class Builder {

		private final RowSetFactory rowSetFactory = new SidecarRowSetFactory();
		private int maxByteBufferCapacity = DEFAULT_BYTE_BUFFER_CAPACITY;

		public Builder maxByteBufferCapacity(int capacity) {
			this.maxByteBufferCapacity = capacity;
			return this;
		}

		public ResultSetCodec build() {
			return new ResultSetCodec(this);
		}

	}

}
