package com.redis.sidecar.core;

import java.sql.ResultSet;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

abstract class AbstractResultSetCache implements ResultSetCache {

	private static final Logger log = Logger.getLogger(AbstractResultSetCache.class.getName());

	private final Timer getTimer = Metrics.timer("cache.gets");
	private final Timer putTimer = Metrics.timer("cache.puts");
	private final Counter missCounter = Metrics.counter("cache.gets", Tags.of("result", "miss"));
	private final Counter hitCounter = Metrics.counter("cache.gets", Tags.of("result", "hit"));

	@Override
	public Optional<ResultSet> get(String key) {
		ResultSet resultSet;
		try {
			resultSet = getTimer.recordCallable(() -> doGet(key));
		} catch (Exception e) {
			log.log(Level.SEVERE, "Could not retrieve key " + key, e);
			return Optional.empty();
		}
		if (resultSet == null) {
			missCounter.increment();
			return Optional.empty();
		}
		hitCounter.increment();
		return Optional.of(resultSet);
	}

	protected abstract ResultSet doGet(String key) throws Exception;

	@Override
	public void put(String key, long ttl, ResultSet resultSet) {
		if (ttl == Config.TTL_NO_CACHE) {
			return;
		}
		try {
			putTimer.recordCallable(() -> doPut(key, ttl, resultSet));
		} catch (Exception e) {
			log.log(Level.SEVERE, "Could not store key " + key, e);
		}
	}

	protected abstract ResultSet doPut(String key, long ttl, ResultSet resultSet) throws Exception;

}
