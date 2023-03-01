package com.redis.smartcache.core;

import java.sql.ResultSet;

public class CachedResultSet {

	private final String key;
	private final Query query;
	private final Action action;
	private ResultSet resultSet;

	public CachedResultSet(String key, Query query, Action action) {
		this.key = key;
		this.query = query;
		this.action = action;
	}

	public String getKey() {
		return key;
	}

	public Query getQuery() {
		return query;
	}

	public Action getAction() {
		return action;
	}

	public boolean hasResultSet() {
		return resultSet != null;
	}

	public ResultSet getResultSet() {
		return resultSet;
	}

	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

}
