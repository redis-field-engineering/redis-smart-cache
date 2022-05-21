package com.redis.sidecar.core;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetCache extends AutoCloseable {

	/**
	 * 
	 * @param key the key to get the ResultSet for.
	 * @return the ResultSet that was retrieved from cache or null if none found.
	 * @throws SQLException if the ResultSet could not be retrieved
	 */
	ResultSet get(String key) throws SQLException;

	/**
	 * Adds a ResultSet to the cache.
	 *
	 * @param key       the key to store the ResultSet under.
	 * @param seconds   key expiration in seconds.
	 * @param resultSet the ResultSet to store under the key.
	 * @throws SQLException if an error occurred while storing the ResultSet
	 */
	void put(String key, long ttl, ResultSet resultSet) throws SQLException;

}
