package com.redis.sidecar.core;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.support.ConnectionPoolSupport;

public class StringResultSetCache<T extends StatefulConnection<String, ResultSet>> implements ResultSetCache {

	private final AtomicLong misses = new AtomicLong();
	private final AtomicLong hits = new AtomicLong();
	private final GenericObjectPool<T> pool;
	private final Function<T, RedisStringCommands<String, ResultSet>> sync;

	public StringResultSetCache(Supplier<T> connectionSupplier, GenericObjectPoolConfig<T> poolConfig,
			Function<T, RedisStringCommands<String, ResultSet>> sync) {
		LettuceAssert.notNull(poolConfig, "Connection pool must not be null");
		LettuceAssert.notNull(sync, "Sync commands must not be null");
		this.pool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, poolConfig);
		this.sync = sync;
	}

	@Override
	public void close() throws Exception {
		pool.close();
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
		try (T connection = pool.borrowObject()) {
			ResultSet value = sync.apply(connection).get(key);
			if (value == null) {
				misses.incrementAndGet();
				return null;
			}
			hits.incrementAndGet();
			return value;
		} catch (IOException e) {
			throw new SQLException("Could not decode ResultSet", e);
		} catch (Exception e) {
			throw new SQLException("Could not get cache connection", e);
		}
	}

	private String key(String sql) {
		return sql;
	}

	@Override
	public void set(String sql, ResultSet resultSet) throws SQLException {
		String key = key(sql);
		try (T connection = pool.borrowObject()) {
			sync.apply(connection).set(key, resultSet);
		} catch (IOException e) {
			throw new SQLException("Could not encode ResultSet", e);
		} catch (Exception e) {
			throw new SQLException("Could not get cache connection", e);
		}

	}

}
