package com.redis.smartcache.jdbc.codec;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;

import io.netty.buffer.ByteBuf;

public class DateColumnCodec extends NullableColumnCodec<Long> {

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