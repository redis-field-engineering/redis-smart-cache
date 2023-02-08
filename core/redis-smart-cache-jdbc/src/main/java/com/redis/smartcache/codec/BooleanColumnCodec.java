package com.redis.smartcache.codec;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.netty.buffer.ByteBuf;

public class BooleanColumnCodec extends NullableColumnCodec<Boolean> {

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