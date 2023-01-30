package com.redis.sidecar;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.sidecar.Config.Rule;

import io.lettuce.core.internal.LettuceAssert;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class ResultSetCacheImpl implements ResultSetCache {

	private static final Logger log = Logger.getLogger(ResultSetCacheImpl.class.getName());

	private static final String METER_PREFIX = "cache.";
	private static final String METER_GETS = METER_PREFIX + "gets";
	private static final String METER_PUTS = METER_PREFIX + "puts";

	private final Timer getTimer;
	private final Timer putTimer;
	private final Counter missCounter;
	private final Counter hitCounter;
	private final String keyspace;

	private final GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>> pool;

	protected ResultSetCacheImpl(MeterRegistry meterRegistry,
			GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>> pool, String keyspace) {
		LettuceAssert.notNull(pool, "Connection pool must not be null");
		this.getTimer = meterRegistry.timer(METER_GETS);
		this.putTimer = meterRegistry.timer(METER_PUTS);
		this.missCounter = meterRegistry.counter(METER_GETS, "result", "miss");
		this.hitCounter = meterRegistry.counter(METER_GETS, "result", "hit");
		this.keyspace = keyspace;
		this.pool = pool;
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

	protected ResultSet doGet(String key) throws Exception {
		try (StatefulRedisModulesConnection<String, ResultSet> connection = pool.borrowObject()) {
			return connection.sync().get(key);
		}
	}

	protected String key(String sql) {
		return keyspace + ":" + crc(sql);
	}

	private final String crc(String string) {
		CRC32 crc = new CRC32();
		crc.update(string.getBytes(StandardCharsets.UTF_8));
		return String.valueOf(crc.getValue());
	}

	@Override
	public void put(String sql, long ttl, ResultSet resultSet) {
		if (ttl == Rule.TTL_NO_CACHE) {
			return;
		}
		String key = key(sql);
		try {
			putTimer.recordCallable(() -> doPut(key, ttl, resultSet));
		} catch (Exception e) {
			log.log(Level.SEVERE, String.format("Could not store key %s", key), e);
		}
	}

	protected ResultSet doPut(String key, long ttl, ResultSet resultSet) throws Exception {
		try (StatefulRedisModulesConnection<String, ResultSet> connection = pool.borrowObject()) {
			connection.sync().setex(key, ttl, resultSet);
		}
		return resultSet;

	}

}
