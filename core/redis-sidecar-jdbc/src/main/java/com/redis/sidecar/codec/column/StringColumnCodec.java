package com.redis.sidecar.codec.column;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.redis.sidecar.codec.StringCodec;

import io.netty.buffer.ByteBuf;

public class StringColumnCodec extends NullableColumnCodec<String> {

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