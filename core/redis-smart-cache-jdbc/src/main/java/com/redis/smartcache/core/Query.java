package com.redis.smartcache.core;

import java.time.Duration;
import java.util.Set;

public class Query {

	public static final Duration TTL_NO_CACHING = Duration.ZERO;
	public static final long TTL_NO_EXPIRATION = -1;

	private final String id;
	private final String sql;
	private final Set<String> tables;
	private Duration ttl = TTL_NO_CACHING;

	public Query(String id, String sql, Set<String> tables) {
		this.id = id;
		this.sql = sql;
		this.tables = tables;
	}

	public String getId() {
		return id;
	}

	public String getSql() {
		return sql;
	}

	public Set<String> getTables() {
		return tables;
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
