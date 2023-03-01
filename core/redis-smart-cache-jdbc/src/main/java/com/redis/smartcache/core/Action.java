package com.redis.smartcache.core;

import java.time.Duration;

public class Action {

	public static final Duration TTL_NO_CACHING = Duration.ZERO;
	public static final long TTL_NO_EXPIRATION = -1;

	private final Query query;
	private Duration ttl = TTL_NO_CACHING;

	public Action(Query query) {
		this.query = query;
	}

	public Query getQuery() {
		return query;
	}

	public Duration getTtl() {
		return ttl;
	}

	public void setTtl(Duration ttl) {
		if (ttl == null || ttl.isNegative()) {
			throw new IllegalArgumentException("TTL duration must be greater than 0");
		}
		this.ttl = ttl;
	}

	public boolean isCaching() {
		return !TTL_NO_CACHING.equals(ttl);
	}
}
