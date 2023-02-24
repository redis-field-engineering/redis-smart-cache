package com.redis.smartcache.core;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetCache {

	/**
	 * 
	 * @param key key to get the ResultSet for.
	 * @return ResultSet that was retrieved from cache or null if none found.
	 */
	ResultSet get(String key);

	/**
	 * Adds a ResultSet to the cache.
	 *
	 * @param key       key to store the ResultSet under.
	 * @param ttl       the key TTL in seconds.
	 * @param resultSet the ResultSet to store under the key.
	 * @throws SQLException if an error occurred while storing the ResultSet
	 */
	void put(String key, long ttl, ResultSet resultSet);

}
