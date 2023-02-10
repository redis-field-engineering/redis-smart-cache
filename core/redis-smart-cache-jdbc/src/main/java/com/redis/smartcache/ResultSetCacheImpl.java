package com.redis.smartcache;

import java.sql.ResultSet;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.internal.LettuceAssert;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class ResultSetCacheImpl implements ResultSetCache {

	private static final Logger log = Logger.getLogger(ResultSetCacheImpl.class.getName());

	private static final String METER_PREFIX = "cache.";
	private static final String METER_GETS = METER_PREFIX + "gets";
	private static final String METER_PUTS = METER_PREFIX + "puts";
	private static final String METER_RESULT_TAG = "result";
	private static final String METER_MISS_TAG = "miss";
	private static final String METER_HIT_TAG = "hit";

	private final Timer getTimer;
	private final Timer putTimer;
	private final Counter missCounter;
	private final Counter hitCounter;

	private final GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>> pool;

	public ResultSetCacheImpl(GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>> pool,
			MeterRegistry meterRegistry) {
		LettuceAssert.notNull(meterRegistry, "Meter registry must not be null");
		LettuceAssert.notNull(pool, "Connection pool must not be null");
		this.pool = pool;
		this.getTimer = meterRegistry.timer(METER_GETS);
		this.putTimer = meterRegistry.timer(METER_PUTS);
		this.missCounter = meterRegistry.counter(METER_GETS, METER_RESULT_TAG, METER_MISS_TAG);
		this.hitCounter = meterRegistry.counter(METER_GETS, METER_RESULT_TAG, METER_HIT_TAG);
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

	protected ResultSet doGet(String key) throws Exception {
		try (StatefulRedisModulesConnection<String, ResultSet> connection = pool.borrowObject()) {
			return connection.sync().get(key);
		}
	}

	@Override
	public void put(String key, long ttl, ResultSet resultSet) {
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
