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
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

public abstract class AbstractRedisResultSetCache implements ResultSetCache {

	protected static final RedisCodec<String, byte[]> REDIS_CODEC = RedisCodec.of(StringCodec.UTF8,
			ByteArrayCodec.INSTANCE);

	private final AtomicLong misses = new AtomicLong();
	private final AtomicLong hits = new AtomicLong();
	private final ResultSetCodec codec = new ResultSetCodec();
	private final GenericObjectPool<StatefulConnection<String, byte[]>> pool;
	private final Function<StatefulConnection<String, byte[]>, BaseRedisCommands<String, byte[]>> sync;

	protected AbstractRedisResultSetCache(Supplier<StatefulConnection<String, byte[]>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<String, byte[]>> poolConfig,
			Function<StatefulConnection<String, byte[]>, BaseRedisCommands<String, byte[]>> sync) {
		this.pool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, poolConfig);
		this.sync = sync;
	}

	@Override
	public void close() throws Exception {
		pool.close();
		doClose();
	}

	protected abstract void doClose();

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
		try (StatefulConnection<String, byte[]> connection = pool.borrowObject()) {
			byte[] value = ((RedisStringCommands<String, byte[]>) sync.apply(connection)).get(key);
			if (value == null) {
				misses.incrementAndGet();
				return null;
			}
			hits.incrementAndGet();
			return codec.decode(value);
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
	public ResultSet set(String sql, ResultSet resultSet) throws SQLException {
		String key = key(sql);
		try (StatefulConnection<String, byte[]> connection = pool.borrowObject()) {
			byte[] value = codec.encode(resultSet);
			((RedisStringCommands<String, byte[]>) sync.apply(connection)).set(key, value);
			return codec.decode(value);
		} catch (IOException e) {
			throw new SQLException("Could not encode ResultSet", e);
		} catch (Exception e) {
			throw new SQLException("Could not get cache connection", e);
		}

	}

}
