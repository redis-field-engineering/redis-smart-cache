package com.redis.smartcache.core.codec;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;

import io.netty.buffer.ByteBuf;

public class TimeColumnCodec extends NullableColumnCodec<Time> {

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