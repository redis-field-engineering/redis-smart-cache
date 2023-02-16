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
	private static final String METER_GET = METER_PREFIX + "get";
	private static final String METER_PUT = METER_PREFIX + "put";
	private static final String TAG_RESULT = "result";
	private static final String TAG_MISS = "miss";
	private static final String TAG_HIT = "hit";

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
		this.getTimer = Timer.builder(METER_GET).publishPercentiles(0.9, 0.99).register(meterRegistry);
		this.putTimer = Timer.builder(METER_PUT).publishPercentiles(0.9, 0.99).register(meterRegistry);
		this.missCounter = Counter.builder(METER_GET).tag(TAG_RESULT, TAG_MISS).register(meterRegistry);
		this.hitCounter = Counter.builder(METER_GET).tag(TAG_RESULT, TAG_HIT).register(meterRegistry);
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
