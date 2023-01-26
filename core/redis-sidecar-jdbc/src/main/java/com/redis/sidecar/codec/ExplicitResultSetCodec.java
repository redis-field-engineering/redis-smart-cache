package com.redis.sidecar.codec;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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

import javax.sql.RowSet;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

import io.lettuce.core.codec.RedisCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ExplicitResultSetCodec implements RedisCodec<String, ResultSet> {

	public static final int DEFAULT_BYTE_BUFFER_CAPACITY = 10000000; // 10 MB
	public static final StringCodec DEFAULT_STRING_CODEC = CharsetStringCodec.UTF8;

	private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

	private final RowSetFactory rowSetFactory;
	private final int maxByteBufferCapacity;
	private final StringCodec stringCodec;

	private ExplicitResultSetCodec(Builder builder) {
		this.rowSetFactory = builder.rowSetFactory;
		this.maxByteBufferCapacity = builder.maxByteBufferCapacity;
		this.stringCodec = builder.stringCodec;
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

	interface ColumnCodec {

		void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException;

		void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException;

	}

	interface StringCodec {

		String decode(ByteBuf buffer);

		void encode(ByteBuf buffer, String string);

	}

	static class CharsetStringCodec implements StringCodec {

		public static final StringCodec UTF8 = CharsetStringCodec.of(StandardCharsets.UTF_8);

		private final Charset charset;

		public CharsetStringCodec(Charset charset) {
			this.charset = charset;
		}

		@Override
		public String decode(ByteBuf buffer) {
			int length = buffer.readInt();
			byte[] bytes = new byte[length];
			buffer.readBytes(bytes);
			return new String(bytes, charset);
		}

		@Override
		public void encode(ByteBuf buffer, String string) {
			byte[] bytes = string.getBytes(charset);
			buffer.writeInt(bytes.length);
			buffer.writeBytes(bytes);
		}

		public static CharsetStringCodec of(Charset charset) {
			return new CharsetStringCodec(charset);
		}
	}

	static class BooleanColumnCodec extends NullableColumnCodec<Boolean> {

		public BooleanColumnCodec(int columnIndex) {
			super(columnIndex);
		}

		@Override
		protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateBoolean(columnIndex, byteBuf.readBoolean());
		}

		@Override
		protected void write(ByteBuf byteBuf, Boolean value) throws SQLException {
			byteBuf.writeBoolean(value);
		}

		@Override
		protected Boolean getValue(ResultSet resultSet) throws SQLException {
			return resultSet.getBoolean(columnIndex);
		}
	}

	static class IntegerColumnCodec extends NullableColumnCodec<Integer> {

		public IntegerColumnCodec(int columnIndex) {
			super(columnIndex);
		}

		@Override
		protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateInt(columnIndex, byteBuf.readInt());
		}

		@Override
		protected void write(ByteBuf byteBuf, Integer value) throws SQLException {
			byteBuf.writeInt(value);
		}

		@Override
		protected Integer getValue(ResultSet resultSet) throws SQLException {
			return resultSet.getInt(columnIndex);
		}
	}

	static class LongColumnCodec extends NullableColumnCodec<Long> {

		public LongColumnCodec(int columnIndex) {
			super(columnIndex);
		}

		@Override
		protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateLong(columnIndex, byteBuf.readLong());
		}

		@Override
		protected void write(ByteBuf byteBuf, Long value) throws SQLException {
			byteBuf.writeLong(value);
		}

		@Override
		protected Long getValue(ResultSet resultSet) throws SQLException {
			return resultSet.getLong(columnIndex);
		}
	}

	static class FloatColumnCodec extends NullableColumnCodec<Float> {

		public FloatColumnCodec(int columnIndex) {
			super(columnIndex);
		}

		@Override
		protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateFloat(columnIndex, byteBuf.readFloat());
		}

		@Override
		protected void write(ByteBuf byteBuf, Float value) throws SQLException {
			byteBuf.writeFloat(value);
		}

		@Override
		protected Float getValue(ResultSet resultSet) throws SQLException {
			return resultSet.getFloat(columnIndex);
		}
	}

	static class DoubleColumnCodec extends NullableColumnCodec<Double> {

		public DoubleColumnCodec(int columnIndex) {
			super(columnIndex);
		}

		@Override
		protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateDouble(columnIndex, byteBuf.readDouble());
		}

		@Override
		protected void write(ByteBuf byteBuf, Double value) throws SQLException {
			byteBuf.writeDouble(value);
		}

		@Override
		protected Double getValue(ResultSet resultSet) throws SQLException {
			return resultSet.getDouble(columnIndex);
		}
	}

	static class StringColumnCodec extends NullableColumnCodec<String> {

		private final StringCodec codec;

		public StringColumnCodec(int columnIndex, StringCodec codec) {
			super(columnIndex);
			this.codec = codec;
		}

		@Override
		protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateString(columnIndex, codec.decode(byteBuf));
		}

		@Override
		protected void write(ByteBuf byteBuf, String value) throws SQLException {
			codec.encode(byteBuf, value);
		}

		@Override
		protected String getValue(ResultSet resultSet) throws SQLException {
			return resultSet.getString(columnIndex);
		}
	}

	static class DateColumnCodec extends NullableColumnCodec<Long> {

		public DateColumnCodec(int columnIndex) {
			super(columnIndex);
		}

		@Override
		protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateDate(columnIndex, new Date(byteBuf.readLong()));
		}

		@Override
		protected void write(ByteBuf byteBuf, Long value) throws SQLException {
			byteBuf.writeLong(value);
		}

		@Override
		protected Long getValue(ResultSet resultSet) throws SQLException {
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

	static class BigDecimalColumnCodec extends NullableColumnCodec<BigDecimal> {

		public BigDecimalColumnCodec(int columnIndex) {
			super(columnIndex);
		}

		@Override
		protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateBigDecimal(columnIndex, BigDecimal.valueOf(byteBuf.readDouble()));
		}

		@Override
		protected void write(ByteBuf byteBuf, BigDecimal value) throws SQLException {
			byteBuf.writeDouble(value.doubleValue());
		}

		@Override
		protected BigDecimal getValue(ResultSet resultSet) throws SQLException {
			return resultSet.getBigDecimal(columnIndex);
		}

	}

	static class TimeColumnCodec extends NullableColumnCodec<Time> {

		public TimeColumnCodec(int columnIndex) {
			super(columnIndex);
		}

		@Override
		protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateTime(columnIndex, new Time(byteBuf.readLong()));
		}

		@Override
		protected void write(ByteBuf byteBuf, Time value) throws SQLException {
			byteBuf.writeLong(value.getTime());
		}

		@Override
		protected Time getValue(ResultSet resultSet) throws SQLException {
			return resultSet.getTime(columnIndex);
		}
	}

	static class TimestampColumnCodec extends NullableColumnCodec<Timestamp> {

		public TimestampColumnCodec(int columnIndex) {
			super(columnIndex);
		}

		@Override
		protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			resultSet.updateTimestamp(columnIndex, new Timestamp(byteBuf.readLong()));
		}

		@Override
		protected void write(ByteBuf byteBuf, Timestamp value) throws SQLException {
			byteBuf.writeLong(value.getTime());
		}

		@Override
		protected Timestamp getValue(ResultSet resultSet) throws SQLException {
			return resultSet.getTimestamp(columnIndex);
		}
	}

	abstract static class NullableColumnCodec<T> implements ColumnCodec {

		protected final int columnIndex;

		protected NullableColumnCodec(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		@Override
		public void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
			boolean nullValue = byteBuf.readBoolean();
			if (nullValue) {
				resultSet.updateNull(columnIndex);
			} else {
				updateValue(byteBuf, resultSet);
			}
		}

		protected abstract void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException;

		@Override
		public void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException {
			T value = getValue(resultSet);
			if (resultSet.wasNull() || value == null) {
				byteBuf.writeBoolean(true);
			} else {
				byteBuf.writeBoolean(false);
				write(byteBuf, value);
			}
		}

		protected abstract void write(ByteBuf byteBuf, T value) throws SQLException;

		protected abstract T getValue(ResultSet resultSet) throws SQLException;

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
			return new StringColumnCodec(columnIndex, stringCodec);
		case Types.DATE:
			return new DateColumnCodec(columnIndex);
		case Types.TIME:
			return new TimeColumnCodec(columnIndex);
		case Types.TIMESTAMP:
			return new TimestampColumnCodec(columnIndex);
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
			metaData.setCatalogName(columnIndex, stringCodec.decode(bytes));
			metaData.setColumnLabel(columnIndex, stringCodec.decode(bytes));
			metaData.setColumnName(columnIndex, stringCodec.decode(bytes));
			metaData.setColumnTypeName(columnIndex, stringCodec.decode(bytes));
			metaData.setColumnType(columnIndex, bytes.readInt());
			metaData.setColumnDisplaySize(columnIndex, bytes.readInt());
			metaData.setPrecision(columnIndex, bytes.readInt());
			metaData.setTableName(columnIndex, stringCodec.decode(bytes));
			metaData.setScale(columnIndex, bytes.readInt());
			metaData.setSchemaName(columnIndex, stringCodec.decode(bytes));
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
			stringCodec.encode(bytes, metaData.getCatalogName(index));
			stringCodec.encode(bytes, metaData.getColumnLabel(index));
			stringCodec.encode(bytes, metaData.getColumnName(index));
			stringCodec.encode(bytes, metaData.getColumnTypeName(index));
			bytes.writeInt(metaData.getColumnType(index));
			bytes.writeInt(metaData.getColumnDisplaySize(index));
			bytes.writeInt(metaData.getPrecision(index));
			stringCodec.encode(bytes, metaData.getTableName(index));
			bytes.writeInt(metaData.getScale(index));
			stringCodec.encode(bytes, metaData.getSchemaName(index));
			bytes.writeBoolean(metaData.isAutoIncrement(index));
			bytes.writeBoolean(metaData.isCaseSensitive(index));
			bytes.writeBoolean(metaData.isCurrency(index));
			bytes.writeInt(metaData.isNullable(index));
			bytes.writeBoolean(metaData.isSearchable(index));
			bytes.writeBoolean(metaData.isSigned(index));
		}
	}

	public static Builder builder() throws SQLException {
		return new Builder();
	}

	public static Builder builder(RowSetFactory rowSetFactory) {
		return new Builder(rowSetFactory);
	}

	public static class Builder {

		private final RowSetFactory rowSetFactory;
		private int maxByteBufferCapacity = DEFAULT_BYTE_BUFFER_CAPACITY;
		private StringCodec stringCodec = DEFAULT_STRING_CODEC;

		public Builder() throws SQLException {
			this(RowSetProvider.newFactory());
		}

		public Builder(RowSetFactory rowSetFactory) {
			this.rowSetFactory = rowSetFactory;
		}

		public Builder maxByteBufferCapacity(int capacity) {
			this.maxByteBufferCapacity = capacity;
			return this;
		}

		public Builder stringCodec(StringCodec codec) {
			this.stringCodec = codec;
			return this;
		}

		public ExplicitResultSetCodec build() {
			return new ExplicitResultSetCodec(this);
		}

	}

}
