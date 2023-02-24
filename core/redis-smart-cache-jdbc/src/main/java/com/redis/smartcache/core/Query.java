package com.redis.smartcache.core;

import io.trino.sql.tree.Statement;

public class Query {

	public static final long TTL_NO_CACHING = 0;
	public static final long TTL_NO_EXPIRATION = -1;

	private final String id;
	private final String sql;
	private final Statement statement;
	private long ttl = TTL_NO_CACHING;

	public Query(String id, String sql, Statement statement) {
		this.id = id;
		this.sql = sql;
		this.statement = statement;
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
