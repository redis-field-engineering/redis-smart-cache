package com.redis.smartcache.core;

public class QueryAction {

	private final String key;
	private final Query query;
	private final Action action;

	public QueryAction(String key, Query query, Action action) {
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

	public boolean isCaching() {
		return action.isCaching();
	}

}
