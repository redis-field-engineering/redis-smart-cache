package com.redis.sidecar.core;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redis.sidecar.Driver;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.internal.LettuceAssert;
import io.micrometer.core.instrument.MeterRegistry;

public class StringResultSetCache<T extends StatefulConnection<String, ResultSet>> extends AbstractResultSetCache {

	private final GenericObjectPool<T> pool;
	private final Function<T, RedisStringCommands<String, ResultSet>> sync;
	private final String keyspace;

	public StringResultSetCache(MeterRegistry meterRegistry, GenericObjectPool<T> pool,
			Function<T, RedisStringCommands<String, ResultSet>> sync, String keyspace) {
		super(meterRegistry);
		LettuceAssert.notNull(pool, "Connection pool must not be null");
		LettuceAssert.notNull(sync, "Sync commands must not be null");
		LettuceAssert.notNull(keyspace, "Keyspace must not be null");
		this.pool = pool;
		this.sync = sync;
		this.keyspace = keyspace;
	}

	@Override
	public void close() throws Exception {
		pool.close();
	}

	private String key(String sql) {
		return keyspace + Driver.KEY_SEPARATOR + sql;
	}

	@Override
	protected ResultSet doGet(String sql) throws SQLException {
		try (T connection = pool.borrowObject()) {
			return sync.apply(connection).get(key(sql));
		} catch (IOException e) {
			throw new SQLException("Could not decode ResultSet", e);
		} catch (Exception e) {
			throw new SQLException("Could not get cache connection", e);
		}
	}

	@Override
	protected ResultSet doPut(String sql, ResultSet resultSet) throws SQLException {
		try (T connection = pool.borrowObject()) {
			sync.apply(connection).set(key(sql), resultSet);
		} catch (IOException e) {
			throw new SQLException("Could not encode ResultSet", e);
		} catch (Exception e) {
			throw new SQLException("Could not get cache connection", e);
		}
		return resultSet;
	}

}
