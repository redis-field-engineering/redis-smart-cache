package com.redis.smartcache.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

import javax.sql.RowSet;

public interface RowSetCache extends AutoCloseable {

	/**
	 * 
	 * @param key the unique key to get the ResultSet for.
	 * @return RowSet that was retrieved from cache or null if none
	 */
	RowSet get(String key);

	RowSet put(String key, Duration ttl, ResultSet resultSet) throws SQLException;

}
