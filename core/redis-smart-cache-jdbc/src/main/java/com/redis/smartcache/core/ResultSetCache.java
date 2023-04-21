package com.redis.smartcache.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

public interface ResultSetCache extends AutoCloseable {

	/**
	 * 
	 * @param key the unique key to get the ResultSet for.
	 * @return RowSet that was retrieved from cache or null if none
	 */
	ResultSet get(String key);

	ResultSet put(String key, Duration ttl, ResultSet resultSet) throws SQLException;

}
