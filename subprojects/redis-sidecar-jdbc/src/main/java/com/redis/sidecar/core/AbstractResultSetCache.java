package com.redis.sidecar.core;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

abstract class AbstractResultSetCache implements ResultSetCache {

	private final Timer getTimer = Metrics.timer("gets");
	private final Timer putTimer = Metrics.timer("puts");
	private final Counter missCounter = Metrics.counter("gets", Tags.of("result", "miss"));
	private final Counter hitCounter = Metrics.counter("gets", Tags.of("result", "hit"));

	@Override
	public ResultSet get(String key) throws SQLException {
		ResultSet resultSet;
		try {
			resultSet = getTimer.recordCallable(() -> doGet(key));
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		}
		if (resultSet == null) {
			missCounter.increment();
		} else {
			hitCounter.increment();
		}
		return resultSet;
	}

	protected abstract ResultSet doGet(String key) throws SQLException;

	@Override
	public void put(String key, long ttl, ResultSet resultSet) throws SQLException {
		if (ttl == Config.TTL_NO_CACHE) {
			return;
		}
		try {
			putTimer.recordCallable(() -> doPut(key, ttl, resultSet));
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	protected abstract ResultSet doPut(String key, long ttl, ResultSet resultSet) throws SQLException;

}
