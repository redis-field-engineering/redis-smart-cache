package com.redis.smartcache.core;

import java.util.Set;

public class Query {

	public static final long TTL_NO_CACHING = 0;
	public static final long TTL_NO_EXPIRATION = -1;

	private final String id;
	private final String sql;
	private final Set<String> tables;
	private long ttl = TTL_NO_CACHING;

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

	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	public boolean isCaching() {
		return ttl != TTL_NO_CACHING;
	}

}
