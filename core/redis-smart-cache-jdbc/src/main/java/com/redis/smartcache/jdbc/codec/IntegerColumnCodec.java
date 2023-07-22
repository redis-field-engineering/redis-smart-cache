package com.redis.smartcache.jdbc.codec;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.netty.buffer.ByteBuf;

public class IntegerColumnCodec extends NullableColumnCodec<Integer> {

    public IntegerColumnCodec(int columnIndex) {
        super(columnIndex);
    }

    @Override
    protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
        resultSet.updateInt(columnIndex, byteBuf.readInt());
    }

    @Override
    protected void write(ByteBuf byteBuf, Integer value) throws SQLException {
        byteBuf.writeInt(value);
    }

    @Override
    protected Integer getValue(ResultSet resultSet) throws SQLException {
        return resultSet.getInt(columnIndex);
    }

}
