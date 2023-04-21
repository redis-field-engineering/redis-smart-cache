package com.redis.smartcache.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

import javax.sql.RowSet;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;

import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;

public class RedisResultSetCache implements ResultSetCache {

	private final RowSetFactory rowSetFactory;
	private final StatefulRedisConnection<String, RowSet> connection;

	public RedisResultSetCache(RowSetFactory rowSetFactory, AbstractRedisClient client,
			RedisCodec<String, RowSet> codec) {
		this.rowSetFactory = rowSetFactory;
		this.connection = RedisModulesUtils.connection(client, codec);
	}

	@Override
	public RowSet get(String key) {
		return connection.sync().get(key);
	}

	@Override
	public RowSet put(String key, Duration ttl, ResultSet resultSet) throws SQLException {
		CachedRowSet cached = rowSetFactory.createCachedRowSet();
		cached.populate(resultSet);
		cached.beforeFirst();
		connection.sync().setex(key, ttl.getSeconds(), cached);
		cached.beforeFirst();
		return cached;
	}

	@Override
	public void close() {
		connection.close();
	}

}
