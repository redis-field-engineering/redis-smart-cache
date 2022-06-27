package com.redis.sidecar.core;

import java.sql.ResultSet;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.internal.LettuceAssert;
import io.micrometer.core.instrument.MeterRegistry;

public class StringResultSetCache extends AbstractResultSetCache {

	private final GenericObjectPool<StatefulConnection<String, ResultSet>> pool;
	private final Function<StatefulConnection<String, ResultSet>, RedisStringCommands<String, ResultSet>> sync;

	public StringResultSetCache(MeterRegistry meterRegistry,
			GenericObjectPool<StatefulConnection<String, ResultSet>> pool,
			Function<StatefulConnection<String, ResultSet>, RedisStringCommands<String, ResultSet>> sync) {
		super(meterRegistry);
		LettuceAssert.notNull(pool, "Connection pool must not be null");
		LettuceAssert.notNull(sync, "Sync commands must not be null");
		this.pool = pool;
		this.sync = sync;
	}

	@Override
	public void close() throws Exception {
		pool.close();
	}

	@Override
	protected ResultSet doGet(String key) throws Exception {
		try (StatefulConnection<String, ResultSet> connection = pool.borrowObject()) {
			return sync.apply(connection).get(key);
		}
	}

	@Override
	protected ResultSet doPut(String key, long ttl, ResultSet resultSet) throws Exception {
		try (StatefulConnection<String, ResultSet> connection = pool.borrowObject()) {
			RedisStringCommands<String, ResultSet> commands = sync.apply(connection);
			commands.setex(key, ttl, resultSet);
		}
		return resultSet;
	}

}
