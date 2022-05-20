package com.redis.sidecar.core;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

abstract class AbstractResultSetCache implements ResultSetCache {

	private final Timer getTimer;
	private final Timer putTimer;
	private final Counter missCounter;
	private final Counter hitCounter;
	private final Counter putCounter;

	protected AbstractResultSetCache(MeterRegistry meterRegistry) {
		this.missCounter = Counter.builder("gets").tag("result", "miss")
				.description("The number of times cache lookup methods have returned null").register(meterRegistry);
		this.hitCounter = Counter.builder("gets").tag("result", "hit")
				.description("The number of times cache lookup methods have returned a cached value.")
				.register(meterRegistry);
		this.putCounter = Counter.builder("puts").description("The number of entries added to the cache")
				.register(meterRegistry);
		this.getTimer = Timer.builder("gets.latency").description("Cache get latency").register(meterRegistry);
		this.putTimer = Timer.builder("puts.latency").description("Cache put latency").register(meterRegistry);
	}

	@Override
	public ResultSet get(String sql) throws SQLException {
		ResultSet resultSet;
		try {
			resultSet = getTimer.recordCallable(() -> doGet(sql));
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

	protected abstract ResultSet doGet(String sql) throws SQLException;

	@Override
	public void put(String sql, ResultSet resultSet) throws SQLException {
		try {
			putTimer.recordCallable(() -> doPut(sql, resultSet));
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		}
		putCounter.increment();
	}

	protected abstract ResultSet doPut(String sql, ResultSet resultSet) throws SQLException;

}