package com.redis.smartcache.codec;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.netty.buffer.ByteBuf;

public class FloatColumnCodec extends NullableColumnCodec<Float> {

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