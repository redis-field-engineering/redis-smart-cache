package com.redis.sidecar.codec;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.netty.buffer.ByteBuf;

public class BigDecimalColumnCodec extends NullableColumnCodec<BigDecimal> {

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