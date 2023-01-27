package com.redis.sidecar.codec.column;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.redis.sidecar.codec.ResultSetCodec;

import io.netty.buffer.ByteBuf;

public class BlobColumnCodec extends NullableColumnCodec<Blob> {

	public BlobColumnCodec(int columnIndex) {
		super(columnIndex);
	}

	@Override
	protected Blob getValue(ResultSet resultSet) throws SQLException {
		return resultSet.getBlob(columnIndex);
	}

	@Override
	protected void write(ByteBuf byteBuf, Blob value) throws SQLException {
		int length;
		try {
			length = Math.toIntExact(value.length());
		} catch (ArithmeticException e) {
			throw new SQLException("BLOB too large", e);
		}
		byte[] bytes = length == 0 ? ResultSetCodec.EMPTY_BYTE_ARRAY : value.getBytes(1, length);
		byteBuf.writeInt(bytes.length);
		byteBuf.writeBytes(bytes);
	}

	@Override
	protected void updateValue(ByteBuf byteBuf, ResultSet resultSet) throws SQLException {
		int length = byteBuf.readInt();
		byte[] bytes = new byte[length];
		byteBuf.readBytes(bytes);
		try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)) {
			resultSet.updateBlob(columnIndex, inputStream);
		} catch (IOException e) {
			throw new SQLException("Could not updated BLOB column " + columnIndex, e);
		}
	}

}