package com.redis.smartcache.jdbc.codec;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.netty.buffer.ByteBuf;

public class LongColumnCodec extends NullableColumnCodec<Long> {

    public LongColumnCodec(int columnIndex) {
        super(columnIndex);
    }

    @Override
    protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
        resultSet.updateLong(columnIndex, byteBuf.readLong());
    }

    @Override
    protected void write(ByteBuf byteBuf, Long value) throws SQLException {
        byteBuf.writeLong(value);
    }

    @Override
    protected Long getValue(ResultSet resultSet) throws SQLException {
        return resultSet.getLong(columnIndex);
    }

}
