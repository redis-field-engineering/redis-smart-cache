package com.redis.smartcache.codec;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.netty.buffer.ByteBuf;

public interface ColumnCodec {

	void decode(ByteBuf byteBuf, ResultSet resultSet) throws SQLException;

	void encode(ResultSet resultSet, ByteBuf byteBuf) throws SQLException;

}