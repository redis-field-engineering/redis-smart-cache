package com.redis.smartcache.jdbc.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import javax.sql.RowSet;
import javax.sql.rowset.CachedRowSet;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class SerializedResultSetCodec implements RedisCodec<String, RowSet> {

	private static final byte[] EMPTY = new byte[0];
	private static final StringCodec STRING_CODEC = StringCodec.UTF8;

	private final int maxByteBufferCapacity;

	public SerializedResultSetCodec(int maxBufferCapacity) {
		this.maxByteBufferCapacity = maxBufferCapacity;
	}

	@Override
	public String decodeKey(ByteBuffer bytes) {
		return STRING_CODEC.decodeKey(bytes);
	}

	@Override
	public ByteBuffer encodeKey(String key) {
		return STRING_CODEC.encodeKey(key);
	}

	@Override
	public RowSet decodeValue(ByteBuffer bytes) {
		try {
			return decode(Unpooled.wrappedBuffer(bytes));
		} catch (Exception e) {
			throw new IllegalStateException("Could not decode RowSet", e);
		}
	}

	private RowSet decode(ByteBuf byteBuf) throws IOException, ClassNotFoundException {
		return decode(byteBuf.array());
	}

	public RowSet decode(byte[] bytes) throws IOException, ClassNotFoundException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		ObjectInputStream is = new ObjectInputStream(in);
		return (CachedRowSet) is.readObject();
	}

	@Override
	public ByteBuffer encodeValue(RowSet resultSet) {
		if (resultSet == null) {
			return ByteBuffer.wrap(EMPTY);
		}
		ByteBuffer buffer = ByteBuffer.allocate(maxByteBufferCapacity);
		ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer);
		byteBuf.clear();
		try {
			byte[] bytes = encode(resultSet);
			byteBuf.writeBytes(bytes);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		int writerIndex = byteBuf.writerIndex();
		buffer.limit(writerIndex);
		return buffer;
	}

	public byte[] encode(RowSet rowSet) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = new ObjectOutputStream(bos);
		out.writeObject(rowSet);
		return bos.toByteArray();
	}

}
