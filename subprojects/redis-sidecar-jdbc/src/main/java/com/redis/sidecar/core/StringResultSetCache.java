package com.redis.sidecar.core;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.internal.LettuceAssert;

public class StringResultSetCache extends AbstractResultSetCache {

	private final GenericObjectPool<StatefulConnection<String, ResultSet>> pool;
	private final Function<StatefulConnection<String, ResultSet>, RedisStringCommands<String, ResultSet>> sync;

	public StringResultSetCache(GenericObjectPool<StatefulConnection<String, ResultSet>> pool,
			Function<StatefulConnection<String, ResultSet>, RedisStringCommands<String, ResultSet>> sync) {
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
	protected ResultSet doGet(String key) throws SQLException {
		try (StatefulConnection<String, ResultSet> connection = pool.borrowObject()) {
			return sync.apply(connection).get(key);
		} catch (IOException e) {
			throw new SQLException("Could not decode ResultSet", e);
		} catch (Exception e) {
			throw new SQLException("Could not get cache connection", e);
		}
	}

	@Override
	protected ResultSet doPut(String key, long ttl, ResultSet resultSet) throws SQLException {
		try (StatefulConnection<String, ResultSet> connection = pool.borrowObject()) {
			RedisStringCommands<String, ResultSet> commands = sync.apply(connection);
			commands.setex(key, ttl, resultSet);
		} catch (IOException e) {
			throw new SQLException("Could not encode ResultSet", e);
		} catch (Exception e) {
			throw new SQLException("Could not get cache connection", e);
		}
		return resultSet;
	}

}
