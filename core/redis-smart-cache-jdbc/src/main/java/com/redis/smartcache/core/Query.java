package com.redis.smartcache.core;

import java.util.Set;

public class Query {

	public enum Type {
		STRING, PARAMETERIZED, STORED_PROCEDURE
	}

	private String id;
	private Type type;
	private String sql;
	private Set<String> tables;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public Set<String> getTables() {
		return tables;
	}

	public void setTables(Set<String> tables) {
		this.tables = tables;
	}

}
