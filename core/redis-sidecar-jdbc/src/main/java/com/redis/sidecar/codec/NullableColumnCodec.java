package com.redis.sidecar.codec;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.redis.sidecar.ColumnCodec;

import io.netty.buffer.ByteBuf;

abstract class NullableColumnCodec<T> implements ColumnCodec {

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