package com.redis.sidecar.core;

import java.sql.ResultSet;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

abstract class AbstractResultSetCache implements ResultSetCache {

	private static final Logger log = Logger.getLogger(AbstractResultSetCache.class.getName());

	private static final String METER_PREFIX = "metrics.cache.";
	private static final String METER_GETS = METER_PREFIX + "gets";
	private static final String METER_PUTS = METER_PREFIX + "puts";

	private final Timer getTimer;
	private final Timer putTimer;
	private final Counter missCounter;
	private final Counter hitCounter;

	protected AbstractResultSetCache(MeterRegistry meterRegistry) {
		this.getTimer = Timer.builder(METER_GETS).register(meterRegistry);
		this.putTimer = Timer.builder(METER_PUTS).register(meterRegistry);
		this.missCounter = Counter.builder(METER_GETS).tag("result", "miss").register(meterRegistry);
		this.hitCounter = Counter.builder(METER_GETS).tag("result", "hit").register(meterRegistry);
	}

	@Override
	public Optional<ResultSet> get(String key) {
		ResultSet resultSet;
		try {
			resultSet = getTimer.recordCallable(() -> doGet(key));
		} catch (Exception e) {
			log.log(Level.SEVERE, String.format("Could not retrieve key %s", key), e);
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
			log.log(Level.SEVERE, String.format("Could not store key %s", key), e);
		}
	}

	protected abstract ResultSet doPut(String key, long ttl, ResultSet resultSet) throws Exception;

}
