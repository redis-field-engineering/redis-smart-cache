package com.redis.sidecar.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.RowSet;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class JdkSerializationResultSetCodec implements RedisCodec<String, ResultSet> {

	private static final byte[] EMPTY = new byte[0];
	private static final StringCodec STRING_CODEC = StringCodec.UTF8;

	private final RowSetFactory rowSetFactory;
	private final int maxByteBufferCapacity;

	public JdkSerializationResultSetCodec(RowSetFactory rowSetFactory, int maxBufferCapacity) {
		this.rowSetFactory = rowSetFactory;
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

	private RowSet decode(ByteBuf byteBuf) throws SQLException, IOException, ClassNotFoundException {
		return decode(byteBuf.array());
	}

	public RowSet decode(byte[] bytes) throws SQLException, IOException, ClassNotFoundException {
		CachedRowSet rowSet = rowSetFactory.createCachedRowSet();
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		ObjectInputStream is = new ObjectInputStream(in);
		rowSet.populate((CachedRowSet) is.readObject());
		return rowSet;
	}

	@Override
	public ByteBuffer encodeValue(ResultSet resultSet) {
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

	public byte[] encode(ResultSet resultSet) throws SQLException, IOException {
		try (CachedRowSet cachedRowSet = rowSetFactory.createCachedRowSet()) {
			cachedRowSet.populate(resultSet, 1);
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput out = new ObjectOutputStream(bos);
			out.writeObject(cachedRowSet);
			return bos.toByteArray();
		}
	}

}
