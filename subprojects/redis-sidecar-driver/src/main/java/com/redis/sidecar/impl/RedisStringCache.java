package com.redis.sidecar.impl;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.sidecar.ResultSetCache;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.support.ConnectionPoolSupport;

public class RedisStringCache implements ResultSetCache {

	private final AtomicLong misses = new AtomicLong();
	private final AtomicLong hits = new AtomicLong();
	private final GenericObjectPool<StatefulConnection<String, ResultSet>> pool;
	private final Function<StatefulConnection<String, ResultSet>, BaseRedisCommands<String, ResultSet>> sync;

	public RedisStringCache(Supplier<StatefulConnection<String, ResultSet>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<String, ResultSet>> poolConfig,
			Function<StatefulConnection<String, ResultSet>, BaseRedisCommands<String, ResultSet>> sync) {
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

	@SuppressWarnings("unchecked")
	@Override
	public ResultSet get(String sql) throws SQLException {
		String key = key(sql);
		try (StatefulConnection<String, ResultSet> connection = pool.borrowObject()) {
			ResultSet value = ((RedisStringCommands<String, ResultSet>) sync.apply(connection)).get(key);
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

	@SuppressWarnings("unchecked")
	@Override
	public void set(String sql, ResultSet resultSet) throws SQLException {
		String key = key(sql);
		try (StatefulConnection<String, ResultSet> connection = pool.borrowObject()) {
			((RedisStringCommands<String, ResultSet>) sync.apply(connection)).set(key, resultSet);
		} catch (IOException e) {
			throw new SQLException("Could not encode ResultSet", e);
		} catch (Exception e) {
			throw new SQLException("Could not get cache connection", e);
		}

	}

}
