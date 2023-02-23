package com.redis.smartcache.core;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.trino.sql.tree.Statement;

public class Query {

	public static final long TTL_NO_CACHING = 0;
	public static final long TTL_NO_EXPIRATION = -1;

	private final String id;
	private final String sql;
	private final Statement statement;
	private final Timer timer;
	private final Timer backendTimer;
	private final Timer cacheGetTimer;
	private final Timer cachePutTimer;
	private final Counter cacheHitCounter;
	private final Counter cacheMissCounter;
	private long ttl = TTL_NO_CACHING;

	public Query(String id, String sql, Statement statement, Timer timer, Timer backendTimer, Timer cacheGetTimer,
			Timer cachePutTimer, Counter cacheHitCounter, Counter cacheMissCounter) {
		this.id = id;
		this.sql = sql;
		this.statement = statement;
		this.timer = timer;
		this.backendTimer = backendTimer;
		this.cacheGetTimer = cacheGetTimer;
		this.cachePutTimer = cachePutTimer;
		this.cacheHitCounter = cacheHitCounter;
		this.cacheMissCounter = cacheMissCounter;
	}

	public Counter getCacheHitCounter() {
		return cacheHitCounter;
	}

	public Counter getCacheMissCounter() {
		return cacheMissCounter;
	}

	public Timer getTimer() {
		return timer;
	}

	public Timer getBackendTimer() {
		return backendTimer;
	}

	public Timer getCacheGetTimer() {
		return cacheGetTimer;
	}

	public Timer getCachePutTimer() {
		return cachePutTimer;
	}

	public boolean hasStatement() {
		return statement != null;
	}

	public Statement getStatement() {
		return statement;
	}

	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	public boolean isCaching() {
		return ttl != TTL_NO_CACHING;
	}

	public String getId() {
		return id;
	}

	public String getSql() {
		return sql;
	}

}
