package com.redis.smartcache.jdbc.codec;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import io.netty.buffer.ByteBuf;

public class TimestampColumnCodec extends NullableColumnCodec<Timestamp> {

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