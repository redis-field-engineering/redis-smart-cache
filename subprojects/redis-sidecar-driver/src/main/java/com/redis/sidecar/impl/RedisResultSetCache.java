package com.redis.sidecar.impl;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

import com.redis.sidecar.ResultSetCache;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisResultSetCache implements ResultSetCache {

	private final AtomicLong misses = new AtomicLong();
	private final AtomicLong hits = new AtomicLong();
	private final ResultSetCodec codec = new ResultSetCodec();
	private final RedisClient client;
	private final StatefulRedisConnection<String, byte[]> connection;

	public RedisResultSetCache(String redisURI) {
		this.client = RedisClient.create(redisURI);
		this.connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
	}

	@Override
	public void close() throws Exception {
		connection.close();
		client.shutdown();
		client.getResources().shutdown();
	}

	@Override
	public long getMisses() {
		return misses.longValue();
	}

	@Override
	public long getHits() {
		return hits.longValue();
	}

	@Override
	public ResultSet get(String sql) throws SQLException {
		String key = key(sql);
		byte[] value = connection.sync().get(key);
		if (value == null) {
			misses.incrementAndGet();
			return null;
		}
		hits.incrementAndGet();
		try {
			return codec.decode(value);
		} catch (IOException e) {
			throw new SQLException("Could not decode ResultSet", e);
		}
	}

	private String key(String sql) {
		return sql;
	}

	@Override
	public ResultSet set(String sql, ResultSet resultSet) throws SQLException {
		String key = key(sql);
		try {
			byte[] value = codec.encode(resultSet);
			connection.sync().set(key, value);
			return codec.decode(value);
		} catch (IOException e) {
			throw new SQLException("Could not encode ResultSet", e);
		}

	}

}
