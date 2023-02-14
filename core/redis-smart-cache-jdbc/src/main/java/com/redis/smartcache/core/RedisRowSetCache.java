package com.redis.smartcache.core;

import javax.sql.RowSet;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.internal.LettuceAssert;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class RedisRowSetCache implements RowSetCache {

	private static final String METER_PREFIX = "cache.";
	private static final String METER_GETS = METER_PREFIX + "gets";
	private static final String METER_PUTS = METER_PREFIX + "puts";
	private static final String METER_RESULT_TAG = "result";
	private static final String METER_MISS_TAG = "miss";
	private static final String METER_HIT_TAG = "hit";

	private final StatefulRedisConnection<String, RowSet> connection;
	private final String prefix;
	private final Timer getTimer;
	private final Timer putTimer;
	private final Counter missCounter;
	private final Counter hitCounter;

	public RedisRowSetCache(StatefulRedisModulesConnection<String, RowSet> connection, String prefix,
			MeterRegistry meterRegistry) {
		LettuceAssert.notNull(connection, "Connection must not be null");
		LettuceAssert.notNull(meterRegistry, "Meter registry must not be null");
		this.connection = connection;
		this.prefix = prefix;
		this.getTimer = meterRegistry.timer(METER_GETS);
		this.putTimer = meterRegistry.timer(METER_PUTS);
		this.missCounter = meterRegistry.counter(METER_GETS, METER_RESULT_TAG, METER_MISS_TAG);
		this.hitCounter = meterRegistry.counter(METER_GETS, METER_RESULT_TAG, METER_HIT_TAG);
	}

	private String key(String id) {
		return prefix + id;
	}

	@Override
	public RowSet get(String id) {
		RowSet rowSet = getTimer.record(() -> connection.sync().get(key(id)));
		if (rowSet == null) {
			missCounter.increment();
		} else {
			hitCounter.increment();
		}
		return rowSet;
	}

	@Override
	public void put(String id, long ttl, RowSet resultSet) {
		putTimer.record(() -> connection.sync().setex(key(id), ttl, resultSet));
	}

}
