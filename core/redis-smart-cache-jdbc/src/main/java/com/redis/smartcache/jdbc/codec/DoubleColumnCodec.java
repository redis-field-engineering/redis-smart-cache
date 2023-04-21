package com.redis.smartcache.jdbc.codec;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.netty.buffer.ByteBuf;

public class DoubleColumnCodec extends NullableColumnCodec<Double> {

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