package com.redis.sidecar.core;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;

import com.redis.sidecar.SidecarDriver;
import com.redis.sidecar.config.Config;

import io.lettuce.core.internal.LettuceAssert;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

abstract class AbstractResultSetCache implements ResultSetCache {

	private static final Logger log = Logger.getLogger(AbstractResultSetCache.class.getName());

	private static final String METER_PREFIX = "cache.";
	private static final String METER_GETS = METER_PREFIX + "gets";
	private static final String METER_PUTS = METER_PREFIX + "puts";

	private final Timer getTimer = Metrics.timer(METER_GETS);
	private final Timer putTimer = Metrics.timer(METER_PUTS);
	private final Counter missCounter = Metrics.counter(METER_GETS, "result", "miss");
	private final Counter hitCounter = Metrics.counter(METER_GETS, "result", "hit");
	private final String keyspace;

	protected AbstractResultSetCache(String keyspace) {
		LettuceAssert.notNull(keyspace, "Keyspace must not be null");
		this.keyspace = keyspace;
	}

	@Override
	public Optional<ResultSet> get(String sql) {
		String key = key(sql);
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

	protected String key(String sql) {
		return SidecarDriver.key(keyspace, crc(sql));
	}

	private final String crc(String string) {
		CRC32 crc = new CRC32();
		crc.update(string.getBytes(StandardCharsets.UTF_8));
		return String.valueOf(crc.getValue());
	}

	protected abstract ResultSet doGet(String key) throws Exception;

	@Override
	public void put(String sql, long ttl, ResultSet resultSet) {
		if (ttl == Config.TTL_NO_CACHE) {
			return;
		}
		String key = key(sql);
		try {
			putTimer.recordCallable(() -> doPut(key, ttl, resultSet));
		} catch (Exception e) {
			log.log(Level.SEVERE, String.format("Could not store key %s", key), e);
		}
	}

	protected abstract ResultSet doPut(String key, long ttl, ResultSet resultSet) throws Exception;

}
