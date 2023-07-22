package com.redis.smartcache.jdbc.codec;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.redis.smartcache.jdbc.RowSetCodec;

import io.netty.buffer.ByteBuf;

public class StringColumnCodec extends NullableColumnCodec<String> {

    public StringColumnCodec(int columnIndex) {
        super(columnIndex);
    }

    @Override
    protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
        resultSet.updateString(columnIndex, RowSetCodec.readString(byteBuf));
    }

    @Override
    protected void write(ByteBuf byteBuf, String value) throws SQLException {
        RowSetCodec.writeString(byteBuf, value);
    }

    @Override
    protected String getValue(ResultSet resultSet) throws SQLException {
        return resultSet.getString(columnIndex);
    }

}
