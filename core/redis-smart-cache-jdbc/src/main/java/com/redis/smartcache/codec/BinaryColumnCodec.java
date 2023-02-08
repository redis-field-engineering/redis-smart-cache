package com.redis.smartcache.codec;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.netty.buffer.ByteBuf;

public class BinaryColumnCodec extends NullableColumnCodec<byte[]> {

	public BinaryColumnCodec(int columnIndex) {
		super(columnIndex);
	}

	@Override
	protected byte[] getValue(ResultSet resultSet) throws SQLException {
		return resultSet.getBytes(columnIndex);
	}

	@Override
	protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
		byte[] bytes = new byte[byteBuf.readInt()];
		byteBuf.readBytes(bytes);
		resultSet.updateBytes(columnIndex, bytes);
	}

	@Override
	protected void write(ByteBuf byteBuf, byte[] value) throws SQLException {
		byteBuf.writeInt(value.length);
		byteBuf.writeBytes(value);
	}

}