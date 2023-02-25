package com.redis.smartcache.core;

import java.sql.ResultSet;
import java.time.Duration;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.internal.LettuceAssert;

public class RedisResultSetCache implements ResultSetCache {

	private final StatefulRedisConnection<String, ResultSet> connection;
	private final String prefix;

	public RedisResultSetCache(StatefulRedisModulesConnection<String, ResultSet> connection, String prefix) {
		LettuceAssert.notNull(connection, "Connection must not be null");
		this.connection = connection;
		this.prefix = prefix;
	}

	private String key(String id) {
		return prefix + id;
	}

	@Override
	public ResultSet get(String id) {
		return connection.sync().get(key(id));
	}

	@Override
	public void put(String id, Duration ttl, ResultSet resultSet) {
		connection.sync().set(key(id), resultSet, SetArgs.Builder.ex(ttl));
	}

}
